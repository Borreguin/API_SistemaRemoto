import re

from app.common.PI_connection.pi_connect import create_pi_point
from app.common.PI_connection.pi_util import search_pi_points_and_values
from app.core.v2CalculationEngine.util import get_pi_server
from app.schemas.RequestSchemas import TagListRequest


def get_values_for_tags_using_regex(_filter:str, regex:str):
    pi_server = get_pi_server()
    tags_values = search_pi_points_and_values(pi_server, _filter)
    if regex is not None and len(regex) > 0:
        tag_name_regex = re.compile(regex)
        tags_values = [p for p in tags_values if tag_name_regex.search(p.get('name', ''))]
    return tags_values


def get_values_for_tags(tag_list: TagListRequest):
    pi_server = get_pi_server()
    result = []
    for tag in tag_list.tags:
        point = create_pi_point(pi_server, tag.tag_name)
        if point.pt is None:
            result.append(dict(name=tag.tag_name, value=None, timestamp=None))
            continue
        snapshot = point.snapshot()
        if isinstance(snapshot, dict):
            result.append(snapshot)
            continue
        value, timestamp = str(snapshot.Value), snapshot.Timestamp.LocalTime
        result.append(dict(name=point.pt.Name, value=value, timestamp=str(timestamp)))
    return result
