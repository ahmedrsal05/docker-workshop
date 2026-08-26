"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from dateutil.relativedelta import relativedelta


def _month_starts(start_date: pd.Timestamp, end_date: pd.Timestamp):
    current = pd.Timestamp(year=start_date.year, month=start_date.month, day=1)
    limit = pd.Timestamp(year=end_date.year, month=end_date.month, day=1)
    while current <= limit:
        yield current
        current = current + relativedelta(months=1)


def _load_month(taxi_type: str, month_start: pd.Timestamp) -> pd.DataFrame:
    url = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"{taxi_type}_tripdata_{month_start.year}-{month_start.month:02d}.parquet"
    )
    raw = pd.read_parquet(url)

    pickup_col = "tpep_pickup_datetime" if taxi_type == "yellow" else "lpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime" if taxi_type == "yellow" else "lpep_dropoff_datetime"

    trips = raw.rename(
        columns={
            pickup_col: "pickup_datetime",
            dropoff_col: "dropoff_datetime",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
        }
    )[
        [
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "fare_amount",
            "payment_type",
        ]
    ].copy()

    trips["taxi_type"] = taxi_type
    return trips


def materialize():
    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"])
    end_date = pd.to_datetime(os.environ["BRUIN_END_DATE"])
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    frames = []
    for taxi_type in taxi_types:
        for month_start in _month_starts(start_date, end_date):
            frames.append(_load_month(taxi_type, month_start))

    if not frames:
        return pd.DataFrame(
            columns=[
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "fare_amount",
                "payment_type",
                "taxi_type",
            ]
        )

    final_dataframe = pd.concat(frames, ignore_index=True)
    final_dataframe["pickup_datetime"] = pd.to_datetime(final_dataframe["pickup_datetime"])
    final_dataframe["dropoff_datetime"] = pd.to_datetime(final_dataframe["dropoff_datetime"])
    mask = (final_dataframe["pickup_datetime"] >= start_date) & (
        final_dataframe["pickup_datetime"] < end_date
    )
    return final_dataframe.loc[mask]
