import json
import time
import logging

from Environment import *
from postgres_connect import POSTGRES_CONNECTION_DETAILS, RAW_TABLE_NAME, CLEANED_TABLE_NAME
from Errors import *
from perform_checks import check_all
from mqtt_connect import client, MQTT_CONNECTION_DETAILS


# Logging Configuration
logging.basicConfig(
    # filename='/home/azureuser/python_files/webscraping/gps_data_cleaning_log.log',
    filename='./gps_log.log',
    level=logging.INFO,
    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s'
)

BATCH_INTERVAL = 300  # Seconds


class VehicleStatus:
    def __init__(self, device_id: str, vehicle_number: str, latitude: float,
                 longitude: float, last_time: datetime.datetime):
        self.device_id = device_id
        self.vehicle_number = vehicle_number
        self.lat = latitude
        self.long = longitude
        self.time = last_time


# All the required info about the vehicle whereabouts primary
vehicle_statuses: Dict[str, VehicleStatus] = dict()

# For Event Streaming
vehicles_changed: Dict[str, VehicleStatus] = dict()

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
    vehicles_changed[RAW_DEVICEID_INDEX] = vehicle_statuses[_record[RAW_DEVICEID_INDEX]]


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


if __name__ == '__main__':
    batch_index = 0

    while True:
        print(f"Starting Execution of Batch {batch_index}")
        start = time.time()
        try:
            # Processing, inserting to Postgres
            with psycopg2.connect(**POSTGRES_CONNECTION_DETAILS) as con:
                with con.cursor() as cur:
                    if batch_index % 12 == 0:  # Every 1 hour
                        global last_processed_time_stamp
                        refresh_vehicle_statuses(vehicle_statuses, cur)
                        last_processed_time_stamp = get_last_processed_time_stamp_from_cleaned(cur)
                        refresh_device_id_triggers(device_id_triggers)

                    query = f"""
                        SELECT * FROM {RAW_TABLE_NAME}
                        WHERE time::timestamp > {str(last_processed_time_stamp)}
                        ORDER BY time::timestamp;
                    """
                    cur.execute(query)
                    for record in cur.fetchall():
                        process_record(record, cur)

            print(f"Batch {batch_index} postgres insertion successful after " +
                  f"{datetime.datetime.now()-start}")

            # Streaming Using MQTT
            with client.connect(**MQTT_CONNECTION_DETAILS):
                for deviceid, status in vehicles_changed:
                    try:
                        for entry in device_id_triggers[deviceid]:
                            env, company_id, vehicle_id = entry
                            client.publish(f'{env}/gps/{company_id}/{vehicle_id}', json.dumps(status))
                    except KeyError:
                        continue
            
            print(f"Batch {batch_index} MQTT Streaming successful")

        except RefreshError as e:
            print(f"Error While Refreshing some parameter")
            logging.log(2, e)
            continue

        except psycopg2.Error as e:
            print(f"Postgres Error Executing {batch_index}")
            print(e)
            logging.log(2, e)
            continue

        except Exception as e:
            print(f"Error Executing {batch_index}")
            print(e)
            logging.log(2, e)

        finally:
            end = time.time()
            print(f"Batch {batch_index} finished execution after {end-start}")
            logging.log(1, f"Batch {batch_index} finished execution after {end-start}")
            vehicles_changed = dict()
            batch_index += 1
            time.sleep(BATCH_INTERVAL)
