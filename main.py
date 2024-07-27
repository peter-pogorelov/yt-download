import time
import click
import queue
import random
import pathlib
import datetime
import threading
import itertools

from concurrent.futures import ThreadPoolExecutor

from dir_utils import fetch_downloaded
from json_utils import load_json, yield_from_group_list, VideoInfo
from yt_utils import fetch_data_from_video, YtDownloadOptions, YtDownloadState

DEFAULT_LOG_FILE_PATH = pathlib.Path("./processed.log")
DEFAULT_JSON_FILE_PATH = pathlib.Path("./step3.json")


class VideoPooledProcessor:
    def __init__(self,
                 max_threads: int = 4,
                 max_timeouts: int = 100,
                 download_mode: str = YtDownloadOptions.SUBTITLES_OR_AUDIO,
                 log_file: pathlib.Path = DEFAULT_LOG_FILE_PATH):
        self.max_threads = max_threads

        self.processing_queue = queue.Queue(max_threads*2)
        self.resulting_queue = queue.Queue(max_threads*2)
        self.download_mode = download_mode
        self.complete_state = False
        self.log_file = log_file
        self.max_timeouts = max_timeouts
        self.timeout_counter = itertools.count()

        self.resulting_pool = ThreadPoolExecutor(1)
        self.processing_pool = ThreadPoolExecutor(max_threads)

    def execute(self):
        self.resulting_pool.submit(self.resulting_task)

        for _ in range(self.max_threads):
            self.processing_pool.submit(self.processing_task)

    def complete(self):
        self.complete_state = True

    def put_task(self, video_info: VideoInfo):
        self.processing_queue.put(video_info)

    def processing_task(self):
        thread_ident = threading.get_ident()
        while not self.complete_state:
            video_info: VideoInfo = self.processing_queue.get(block=True)
            print(f"{datetime.datetime.utcnow()} :: Video {video_info.youtube_id} is consumed by {thread_ident}.")
            state = fetch_data_from_video(video_info, self.download_mode)
            self.resulting_queue.put((video_info, state, threading.get_ident()))

    def resulting_task(self):
        with open(self.log_file, "a") as fhandle:
            while not self.complete_state:
                (video_info, state, procuder) = self.resulting_queue.get(block=True)

                if state == YtDownloadState.TIMEOUT:
                    if next(self.timeout_counter) == self.max_timeouts:
                        print(f"Reached max number of timeouts {self.max_timeouts}.")
                        self.complete()

                print(f"{datetime.datetime.utcnow()} :: Video {video_info.youtube_id} finished with state {state} by {procuder}.")
                fhandle.write(video_info.youtube_id + "," + state + "\n")
                fhandle.flush()


@click.command()
@click.option("--json-path", default=DEFAULT_JSON_FILE_PATH, type=pathlib.Path)
@click.option("--log-path", default=DEFAULT_LOG_FILE_PATH, type=pathlib.Path)
@click.option("--download-mode", default=YtDownloadOptions.SUBTITLES_ONLY, type=click.Choice([
    YtDownloadOptions.SUBTITLES_ONLY,
    YtDownloadOptions.AUDIO_ONLY,
    YtDownloadOptions.SUBTITLES_OR_AUDIO,
    YtDownloadOptions.SUBTITLES_AND_AUDIO
]))
@click.option("--sleep-min", default=0, type=int)
@click.option("--sleep-max", default=5, type=int)
@click.option("--start", default=0, type=int)
@click.option("--end", default=-1, type=int)
@click.option("--threads", default=1, type=int)
@click.option("--max-timeouts", default=150, type=int)
def main(json_path: pathlib.Path,
         log_path: pathlib.Path,
         download_mode: str,
         sleep_min: int,
         sleep_max: int,
         start: int,
         end: int,
         threads: int,
         max_timeouts: int):
    group_list = load_json(json_path)
    fetched_videos = fetch_downloaded(log_path)
    pooled_processor = VideoPooledProcessor(
        max_threads=threads, log_file=log_path, download_mode=download_mode, max_timeouts=max_timeouts
    )
    pooled_processor.execute()

    try:
        for yt_data in yield_from_group_list(group_list, fetched_videos, start, end):
            if sleep_max != 0:
                time.sleep(random.randint(sleep_min, sleep_max))

            pooled_processor.put_task(yt_data)
    finally:
        pooled_processor.complete()

    print("Completed.")


if __name__ == "__main__":
    main()
