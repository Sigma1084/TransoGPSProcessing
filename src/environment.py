from typing import List, Dict, Tuple, Any
import psycopg2
from psycopg2.extensions import cursor as pg_cursor
import datetime
import requests
from json.decoder import JSONDecodeError

from core import *
from connections import POSTGRES_CONNECTION_DETAILS, RAW_TABLE_NAME, CLEANED_TABLE_NAME

RAW_COLUMNS: List[str] = None
CLEANED_COLUMNS: List[str] = None

# Some Variables to get to know indices for the tables
RAW_DEVICEID_INDEX: int = None
RAW_VEHICLE_NUMBER_INDEX: int = None
RAW_LAT_INDEX: int = None
RAW_LONG_INDEX: int = None
RAW_TIME_INDEX: int = None
RAW_SPEED_INDEX: int = None
RAW_DISTANCE_INDEX: int = None

CLEANED_VEHICLE_NUMBER_INDEX: int = None
CLEANED_TIME_INDEX: int = None

# NEW_FROM_OLD[i] gives j, _old_record[j] maps to _new_record[i]
NEW_FROM_OLD: Dict[int, int] = dict()


# Marked Private and is not exported
def _refresh_schema(_cursor: pg_cursor):
    """
    Refreshes the Schema and updates some state variables of the program.
    Function is run only in the beginning

    :param _cursor: Postgres Cursor
    :return: None
    """

    query_raw = f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{RAW_TABLE_NAME}';
    """
    _cursor.execute(query_raw)
    res = _cursor.fetchall()
    global RAW_COLUMNS, RAW_DEVICEID_INDEX, RAW_VEHICLE_NUMBER_INDEX, \
        RAW_LAT_INDEX, RAW_LONG_INDEX, RAW_TIME_INDEX, RAW_SPEED_INDEX, RAW_DISTANCE_INDEX
    RAW_COLUMNS = [col[0] for col in res]
    RAW_DEVICEID_INDEX = RAW_COLUMNS.index('deviceid')
    RAW_VEHICLE_NUMBER_INDEX = RAW_COLUMNS.index('vehicle_number')
    RAW_LAT_INDEX = RAW_COLUMNS.index('latitude')
    RAW_LONG_INDEX = RAW_COLUMNS.index('longitude')
    RAW_TIME_INDEX = RAW_COLUMNS.index('time')
    RAW_SPEED_INDEX = RAW_COLUMNS.index('speed')
    RAW_DISTANCE_INDEX = RAW_COLUMNS.index('distance')

    query_cleaned = f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{CLEANED_TABLE_NAME}';
    """
    _cursor.execute(query_cleaned)
    res = _cursor.fetchall()
    global CLEANED_COLUMNS, CLEANED_VEHICLE_NUMBER_INDEX, CLEANED_TIME_INDEX
    CLEANED_COLUMNS = [col[0] for col in res]
    CLEANED_VEHICLE_NUMBER_INDEX = RAW_COLUMNS.index('vehicle_number')
    CLEANED_TIME_INDEX = RAW_COLUMNS.index('time')

    for i, col in enumerate(CLEANED_COLUMNS):
        NEW_FROM_OLD[i] = RAW_COLUMNS.index(col)


def refresh_vehicle_statuses(vehicle_statuses: Dict[str, VehicleStatus], _cursor: pg_cursor) -> None:
    """
    Get the status of every vehicle in CLEANED_TABLE to refresh
    Very Costly Operation that takes around 20 seconds if the table is big

    :param _cursor: Postgres Cursor
    :param vehicle_statuses: The current Vehicle Statuses
    :return: The Vehicles Statuses of all the vehicles as a Dictionary indexed by device_id
    :rtype: None
    """

    _query = f"""
        SELECT DISTINCT ON (deviceid) deviceid, vehicle_number,
            latitude, longitude, time::timestamp
        FROM {CLEANED_TABLE_NAME}
        ORDER BY deviceid, time::timestamp DESC;
    """

    try:
        _cursor.execute(_query)
        _res: List[Tuple[Any, ...]] = _cursor.fetchall()
        for _rec in _res:
            # _rec[0] is the deviceid
            device_id = str(_rec[0])
            vehicle_statuses[device_id] = VehicleStatus(*_rec)

    except psycopg2.Error as _e:
        print("Error while refreshing vehicle statuses")
        raise psycopg2.Error(_e)

    except Exception as e:
        raise RefreshError("Error while refreshing Vehicle Status")


def get_last_processed_time_stamp_from_cleaned(_cursor: pg_cursor) -> datetime.timedelta:
    """
    Refreshes the time stamp of the last processed record in CLEANED_TABLE
    If there are no records in the processed CLEANED_TABLE, stores the time 48 years ago from now

    :param _cursor: Postgres Cursor
    :return: TimeStamp of the latest entered record into the Cleaned Table. (1 day ago if no record is present)
    :rtype: datetime.timedelta
    """

    last_processed_time_stamp = None
    _query = f"""
        SELECT time::timestamp FROM {CLEANED_TABLE_NAME}
        ORDER BY time::timestamp DESC
        LIMIT 1;
    """
    try:
        _cursor.execute(_query)
        last_processed_time_stamp = _cursor.fetchall()[0][0]
        print("Successfully fetched last processed date time")
    except Exception:
        last_processed_time_stamp = datetime.datetime.now() - datetime.timedelta(days=1)
        print("Last processed time stamp could not be fetched, returning to 1 days ago")
    finally:
        print("Finished fetching last processed time stamp")
        return last_processed_time_stamp


def refresh_device_id_triggers(device_id_triggers) -> Dict[str, List[Tuple[str, str, str]]]:
    """
    Returns all the triggers for all the devices.
    A Trigger for a device_id is in the form (env, company_id, vehicle_id) which is a Tuple[str, str, str].
    While event streaming, every trigger is processed separately

    :param device_id_triggers: The device_id_triggers for
    :raise requests.HTTPError: When the loading fails
    :return: A Dictionary containing a list of all the triggers for the particular device_id
    :rtype: Dict[str, List[Tuple[str, str, str]]]
    """

    urls = {
        'dev': "https://dev.transo.in/vehicleMap",
        'uat': "https://uat.transo.in/vehicleMap",
        'app': "https://app.transo.in/vehicleMap"
    }

    for _env, _url in urls.items():
        try:
            data: List[Dict[str, Any]] = requests.get(_url).json()['data']
            for _entry in data:
                try:
                    device_id_triggers[_entry['device_id']].append(
                        (_env, _entry['company_id'], _entry['vehicle_id'])
                    )
                except:
                    device_id_triggers[_entry['device_id']] = [
                        (_env, _entry['company_id'], _entry['vehicle_id'])
                    ]
            print(f"Successful Loaded {_env} Vehicle-Company Mapping")
        except requests.HTTPError as _e:
            print(f"HTTP Error during {_env} Vehicle-Company Mapping")
            raise requests.HTTPError(_e)
        except JSONDecodeError as _e:
            print(f"Unsuccessful JSON Decoding while Loading {_env} Vehicle-Company Mapping")
            raise _e


with psycopg2.connect(**POSTGRES_CONNECTION_DETAILS) as _con:
    with _con.cursor() as _cur:
        _refresh_schema(_cur)
