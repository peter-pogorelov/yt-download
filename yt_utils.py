import os
import subprocess

from json_utils import VideoInfo


class YtDownloadState:
    FAILED = "FAILED"
    SUBTITLES_DOWNLOADED = "SUBTITLES"
    AUDIO_DOWNLOADED = "AUDIO"
    VIDEO_DOWNLOADED = "VIDEO"


def download_subtitles(video_id: str, yt_url: str) -> bool:
    download_path = f"./download/{video_id}/subtitles.ru.srt"

    cmd = \
        f"""yt-dlp \
--quiet \
--output "download/{video_id}/subtitles" \
--skip-download \
--write-subs \
--write-auto-subs \
--sub-lang "ru*" \
--sub-format ttml \
--convert-subs srt \
'{yt_url}'"""

    proc = subprocess.Popen(cmd, shell=True)
    proc.communicate(timeout=10)

    return proc.returncode == 0 and os.path.exists(download_path)


def process_subtitles(video_id: str) -> bool:
    processed_path = f"./download/{video_id}/text.txt"

    cmd = """cat \
"download/{<replace_marker>}/subtitles.ru.srt" |\
sed \
-e '/^[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9] --> [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]$/d' \
-e '/^[[:digit:]]\{1,3\}$/d' \
-e 's/<[^>]*>//g' \
-e '/^[[:space:]]*$/d' > "download/{<replace_marker>}/text.txt"
""".replace("{<replace_marker>}", video_id)

    proc = subprocess.Popen(cmd, shell=True)
    proc.communicate(timeout=5)

    return proc.returncode == 0 and os.path.exists(processed_path)


def download_audio(video_id: str, yt_url: str) -> bool:
    download_path = f"./download/{video_id}/audio.mp3"

    cmd = \
        f"""yt-dlp \
--quiet \
--output "download/{video_id}/audio" \
--extract-audio \
--audio-quality "128K" \
--audio-format "mp3" \
'{yt_url}'
"""

    proc = subprocess.Popen(cmd, shell=True)
    proc.communicate(timeout=180)

    return proc.returncode == 0 and os.path.exists(download_path)


def fetch_data_from_video(video_info: VideoInfo, only_subtitles: bool = False) -> str:
    if download_subtitles(video_info.youtube_id, video_info.url):
        if process_subtitles(video_info.youtube_id):
            return YtDownloadState.SUBTITLES_DOWNLOADED
    elif not only_subtitles and download_audio(video_info.youtube_id, video_info.url):
        return YtDownloadState.AUDIO_DOWNLOADED

    return YtDownloadState.FAILED

