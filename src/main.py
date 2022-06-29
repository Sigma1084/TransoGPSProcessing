import json
import time
import logging
import datetime
from typing import Set, Dict, List, Tuple, Any
import psycopg2
from psycopg2.extensions import cursor as pg_cursor

from core import *

from environment import CLEANED_COLUMNS, NEW_FROM_OLD, RAW_DEVICEID_INDEX, \
    RAW_VEHICLE_NUMBER_INDEX, RAW_LONG_INDEX, RAW_LAT_INDEX, RAW_TIME_INDEX, RAW_SPEED_INDEX, \
    refresh_vehicle_statuses, refresh_device_id_triggers, get_last_processed_time_stamp_from_cleaned

from perform_checks import check_all
from connections import *

# Logging Configuration
logging.basicConfig(
    # filename='/home/azureuser/python_files/webscraping/gps_data_cleaning_log.log',
    filename='../gps_log.log',
    level=logging.INFO,
    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s'
)

BATCH_INTERVAL = 10  # Seconds


# All the required info about the vehicle whereabouts primary
vehicle_statuses: Dict[str, VehicleStatus] = dict()

# For Event Streaming
vehicles_changed: Set[str] = set()

# Time of the latest processed field
last_processed_time_stamp: datetime.datetime = None

# Vehicle To Company ID (dev, uat, prod) Keeps Refreshing
# Every DeviceID contains a List of Tuples of the form (env, company_id, vehicle_id)
device_id_triggers: Dict[str, List[Tuple[str, str, str]]] = dict()

# Refresh Functions


def insert_into_cleaned(_record: List[Any], _cursor: pg_cursor):
    """
    Inserts the record with RAW_SCHEMA into the CLEANED_TABLE

    :param _record: The record with RAW_SCHEMA
    :param _cursor: The cursor for execution to take place
    :return: None
    :rtype: None
    """

    new_record = list()
    for i in range(len(CLEANED_COLUMNS)):
        new_record.append(_record[NEW_FROM_OLD[i]])

    _query = f"""
        INSERT INTO {CLEANED_TABLE_NAME} 
        ({','.join(CLEANED_COLUMNS)}) 
        VALUES 
        ({','.join(new_record)});
    """
    _cursor.execute(_query)


def insert_and_update(_record: List[Any], _cursor: pg_cursor):

    insert_into_cleaned(_record, _cursor)

    # Update the vehicle_status
    # !TODO Improvement
    """
    ### Idea For Improvement On Update:
    - Past 5 Records can be saved in the status
    - Calculating status based on the past 5 records like average speed
    - A Monitoring system made easier with 5 records
    """

    # Updating the vehicle_status of the current vehicle
    vehicle_statuses[_record[RAW_DEVICEID_INDEX]] = VehicleStatus(
        _record[RAW_DEVICEID_INDEX], _record[RAW_VEHICLE_NUMBER_INDEX],
        _record[RAW_LAT_INDEX], _record[RAW_LONG_INDEX], _record[RAW_SPEED_INDEX]
    )

    # Update the vehicles_changed
    vehicles_changed.add(_record[RAW_DEVICEID_INDEX])


def process_record(_record: List[Any], _cursor: pg_cursor):
    if type(_record) == tuple:
        _record = list(_record)
    try:
        prev = None
        try:
            prev = vehicle_statuses[_record[RAW_DEVICEID_INDEX]]
        except:
            pass
        check_all(_record, prev)

        # The _record has passed all the checks and hence needs to be inserted
        insert_and_update(_record, _cursor)

        # If batch Processing is completely taken down, Stream Here
        # client.publish()

    except CheckException as ch_err:
        # Ignored
        logging.log(2, ch_err)

    except PrevNotFound or PrevTooOld as _e:
        logging.log(2, _e)
        insert_and_update(_record, _cursor)

    finally:
        # Updating the Time Stamp
        global last_processed_time_stamp
        last_processed_time_stamp = _record[RAW_TIME_INDEX]


def main():
    batch_index = 0

    while True:
        start = time.time()
        proceed = True
        try:
            # Processing, inserting to Postgres
            with psycopg2.connect(**POSTGRES_CONNECTION_DETAILS) as con:
                with con.cursor() as cur:
                    if batch_index % 12 == 0:  # Every 1 hour
                        print("Starting the Hourly Refresh")
                        refresh_vehicle_statuses(vehicle_statuses, cur)
                        global last_processed_time_stamp
                        last_processed_time_stamp = get_last_processed_time_stamp_from_cleaned(cur)
                        refresh_device_id_triggers(device_id_triggers)
                        print("Ending the Hourly Refresh\n\n")

                    print(f"Starting Execution of Batch {batch_index}")
                    query = f"""
                            SELECT * FROM {RAW_TABLE_NAME}
                            WHERE time::timestamp > '{str(last_processed_time_stamp)}'
                            ORDER BY time::timestamp;
                        """
                    cur.execute(query)
                    for record in cur.fetchall():
                        process_record(record, cur)

            print(f"Batch {batch_index} postgres insertion successful after " +
                  f"{time.time() - start}")

            # Streaming Using MQTT
            client.connect(**MQTT_CONNECTION_DETAILS)
            for deviceid in vehicles_changed:
                try:
                    for entry in device_id_triggers[deviceid]:
                        env, company_id, vehicle_id = entry
                        client.publish(f'{env}/gps/{company_id}/{vehicle_id}',
                                       json.dumps(vehicle_statuses[deviceid]))
                except KeyError:
                    continue
            client.disconnect()

            print(f"Batch {batch_index} MQTT Streaming successful")

        except RefreshError as e:
            print(f"Error While Refreshing some parameter in Batch {batch_index}")
            logging.log(2, e)
            proceed = False
            continue

        except psycopg2.Error as e:
            print(f"Postgres Error Executing Batch {batch_index}")
            print(e)
            logging.log(2, e)

        except ConnectionRefusedError as e:
            print(f"MQTT Connection Refused at batch {batch_index}")
            logging.log(2, e)

        except Exception as e:
            print(f"Unexpected Error Executing Batch {batch_index}")
            print(e)
            logging.log(2, e)
            raise e

        finally:
            end = time.time()
            print(f"Batch {batch_index} finished execution after {end - start}")
            logging.log(1, f"Batch {batch_index} finished execution after {end - start}")
            vehicles_changed.clear()
            if proceed:
                batch_index += 1
            print()
            time.sleep(BATCH_INTERVAL)


if __name__ == '__main__':
    main()
