from geopy import distance
from typing import Tuple, List, Any
from datetime import timedelta
from environment import RAW_LAT_INDEX, RAW_LONG_INDEX, RAW_TIME_INDEX, RAW_SPEED_INDEX, RAW_DISTANCE_INDEX
from core import *

MIN_LAT = 8.20  # Minimum Latitude in India is 8.4
MAX_LAT = 37.8  # Maximum Latitude of India is 37.6
MIN_LONG = 68.5  # Minimum Longitude in India is 68.7
MAX_LONG = 97.5  # Maximum Longitude in India is 97.25

MAX_ALLOWED_SPEED = 100  # Maximum speed allowed for the


def calc_dist(pos1: Tuple[float, float], pos2: Tuple[float, float]) -> float:
    """
    Calculates the great circle distance between 2 positions using geopy module

    :param pos1: Position 1
    :type pos1: Tuple[float, float]
    :param pos2: Position 2
    :type pos2: Tuple[float, float]
    :return: The distance between pos1 and pos2
    :rtype: float
    """

    return distance.great_circle(pos1, pos2).meters


def check_india(lat: float, long: float) -> bool:
    """
    Used to discard positions away from India

    :param lat: Latitude of the point
    :type lat: float
    :param long: Longitude of the point
    :type long: float
    :return: False, then vehicle is not present in India
    :rtype: bool
    """

    return MIN_LAT < float(lat) < MAX_LAT and MIN_LONG < float(long) < MAX_LONG


def calc_speed(lat_new: float, long_new: float, lat_old: float, long_old: float,
               time_difference: timedelta) -> float:
    """
    Calculates the speed using the positions and calc_dist method

    :param lat_new: Latitude of the new point
    :type lat_new: float
    :param long_new: Longitude of the new point
    :type long_new: float
    :param lat_old: Latitude of the old point
    :type lat_old: float
    :param long_old: Longitude of the old point
    :type long_old: float
    :param time_difference: Time difference between the records
    :type time_difference: datetime.timedelta
    :return: The calculated speed of the vehicle using calc_dist
    :rtype: float
    """

    dist: float = calc_dist((lat_old, long_old), (lat_new, long_new))
    return dist / time_difference.total_seconds()


def check_all(_record: List[Any], prev: VehicleStatus) -> None:
    """
    Check if the record is eligible to be inserted.
    Raises some exception when the data is not eligible to be inserted

    :param _record: The Data in Old Table
    :type _record: Tuple[Any, ...]
    :param prev: The information of the previous record of this vehicle
    :type prev: VehicleStatus

    :raise CheckException(reason): When the data is not eligible and needs to be discarded
    :raise PrevTooOld(reason): When the previous data is too old
    :raise PrevNotFound(reason): When the previous data is not found

    :return: None
    """

    if not check_india(_record[RAW_LAT_INDEX], _record[RAW_LONG_INDEX]):
        raise CheckException("Vehicle Not Present in India")

    if prev is not None:
        if prev.time - _record[RAW_TIME_INDEX] > timedelta(minutes=1):
            raise PrevTooOld("The Previous Record is more than 1 minute old")

        # Updating the Distance of the Record
        _record[RAW_DISTANCE_INDEX] = calc_dist((_record[RAW_LAT_INDEX], _record[RAW_LONG_INDEX]),
                                                (prev.lat, prev.long))
    else:
        raise PrevNotFound("Previous Record Not Found")

    calculated_speed = calc_speed(_record[RAW_LAT_INDEX], _record[RAW_LONG_INDEX],
                                  prev.lat, prev.long,
                                  _record[RAW_TIME_INDEX] - prev.time)

    if calculated_speed > MAX_ALLOWED_SPEED:
        raise CheckException("Speed calculated using the records exceeded the limit")

    if _record[RAW_SPEED_INDEX] is None:
        _record[RAW_SPEED_INDEX] = calculated_speed


__all__ = ['check_all']
