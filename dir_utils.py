import os
import pathlib


def fetch_downloaded(log_file: pathlib.Path, retry_states: set[str]) -> set[str]:
    if not os.path.exists(log_file):
        return set()

    processed_videos = list()

    with open(log_file, "r") as fhandle:
        line = fhandle.readline()

        if not line:
            return set()

        video_id, state = line.split(",")

        if state not in retry_states:
            processed_videos.append(video_id)

    return set(processed_videos)


def ensure_exists(video_id: str):
    if (os.path.exists(f"download/{video_id}/text.txt") or
            os.path.exists(f"download/{video_id}/audio.mp3")):
        return True

    return False
