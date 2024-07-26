# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import json
import pathlib
import collections
import dataclasses

from pytimeparse.timeparse import timeparse


class JsonKeys:
    OK_TOPICS = 'topics'
    YT_ID = "youtube_id"
    YT_URL = "url"
    YT_DURATION = "duration"


@dataclasses.dataclass
class VideoInfo:
    youtube_id: str
    url: str
    duration: float


def load_json(jspath: pathlib.Path) -> list:
    with open(jspath, "r") as fjs:
        return json.load(fjs)


def yield_from_group_list(group_list: list, fetched_videos: set, start: int = 0, end: int = -1) -> collections.abc.Iterable[VideoInfo]:
    current_line = 0

    for group in group_list:
        topic_list = group[JsonKeys.OK_TOPICS]
        for topic in topic_list:
            current_line += 1

            if current_line < start:
                continue

            if end != -1 and current_line > end:
                break

            if topic[JsonKeys.YT_ID] in fetched_videos:
                continue

            duration = topic[JsonKeys.YT_DURATION]
            if duration is not None:
                duration = timeparse(topic[JsonKeys.YT_DURATION])

            yield VideoInfo(
                topic[JsonKeys.YT_ID],
                topic[JsonKeys.YT_URL],
                duration
            )
