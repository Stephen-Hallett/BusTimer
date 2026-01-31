"""
All the different work "trips" Have been gathered in exploration.py.
In order to estimate the time until a bus reaches my stop, I need some information about
where the busses actually are. This script tests if I can use the realtime api to determine
the last bus stop a vehicle was at, and the next bus stop they are going to.


example_response = {
    "status": "OK",
    "response": {
        "header": {
            "timestamp": 1769309879.031,
            "gtfs_realtime_version": "1.0",
            "incrementality": 0,
        },
        "entity": [
            {
                "id": "1286-79821-54600-2-e30bf04a",
                "trip_update": {
                    "trip": {
                        "trip_id": "1286-79821-54600-2-e30bf04a",
                        "start_time": "15:10:00",
                        "start_date": "20260125",
                        "schedule_relationship": 0,
                        "route_id": "INN-202",
                        "direction_id": 0,
                    },
                    "stop_time_update": {
                        "stop_sequence": 31,
                        "departure": {
                            "delay": 218,
                            "time": 1769309824,
                            "uncertainty": 0,
                        },
                        "stop_id": "7179-9b4fd003",
                        "schedule_relationship": 0,
                    },
                    "vehicle": {
                        "id": "14325",
                        "label": "NB4325",
                        "license_plate": "HGH742",
                    },
                    "timestamp": 1769309824,
                    "delay": 233,
                },
                "is_deleted": False,
            }
        ],
    },
    "error": None,
}
# NOTE: the stop_sequence, is the last stop the bus was at.
# NOTE: one trip starts when another stops. On sunday 4:30, there were 12 trips going at once.


"""  # NOQA

import os
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint

import polars as pl
import requests

trips = pl.read_json("work_trips.json")
pprint(trips)


def get_trip(trip_id: str) -> None | dict:
    res = requests.get(
        "https://api.at.govt.nz/realtime/legacy/tripupdates",
        params={"tripid": trip_id},
        headers=headers,
        timeout=5,
    ).json()
    if len(res["response"]["entity"]):
        return res["response"]["entity"][0]
    return None


if __name__ == "__main__":
    headers = {"Ocp-Apim-Subscription-Key": os.environ["SUBSCRIPTION_KEY"]}

    with ThreadPoolExecutor(max_workers=10) as executor:
        active_trips = list(
            executor.map(
                get_trip, [trip["trip_id"] for trip in trips.iter_rows(named=True)]
            )
        )

pprint([trip for trip in active_trips if trip is not None])
print(len([trip for trip in active_trips if trip is not None]))
