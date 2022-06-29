from datetime import datetime


class VehicleStatus:
    def __init__(self, device_id: str, vehicle_number: str, latitude: float,
                 longitude: float, last_time: datetime):
        self.device_id = device_id
        self.vehicle_number = vehicle_number
        self.lat = latitude
        self.long = longitude
        self.time = last_time


__all__ = ['VehicleStatus']
