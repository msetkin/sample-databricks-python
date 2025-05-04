from datetime import datetime
from typing import Dict

from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

pep_pickup_date_txt = "2024-01-08"
processing_ts = datetime.now()


class BaseTestData(BaseModel):
    _model_config = ConfigDict(populate_by_name=True, extra="forbid", strict=True)  # type: ignore


class NYTaxiRideBronze(BaseTestData):
    DOLocationID: int
    PULocationID: int
    RatecodeID: int
    VendorID: int
    congestion_surcharge: float
    extra: float
    fare_amount: float
    improvement_surcharge: float = Field(default=0.3)
    mta_tax: float = Field(default=0.5)
    passenger_count: int
    payment_type: int
    store_and_fwd_flag: str = Field(default="N")
    tip_amount: float
    tolls_amount: float
    total_amount: float
    tpep_dropoff_datetime: datetime
    tpep_pickup_datetime: datetime
    trip_distance: float


# Note: with Pydantic>=2.0.0 there will be an opportunity to generate spark schemas directly out of pydantic classes,
# using sparkdantic project (https://github.com/mitchelllisle/sparkdantic)
ny_taxi_bronze_schema = StructType(
    [
        StructField("DOLocationID", IntegerType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("VendorID", IntegerType(), True),
        StructField("congestion_surcharge", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("improvement_surcharge", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("trip_distance", FloatType(), True),
    ]
)

ny_taxi_bronze_sample_ride = NYTaxiRideBronze(
    DOLocationID=236,
    PULocationID=132,
    RatecodeID=2,
    VendorID=2,
    congestion_surcharge=2.5,
    extra=0,
    fare_amount=52,
    passenger_count=1,
    payment_type=1,
    tip_amount=9,
    tolls_amount=6.12,
    total_amount=70.42,
    tpep_dropoff_datetime=datetime(2019, 12, 19, 15, 30),
    tpep_pickup_datetime=datetime(2019, 12, 19, 15, 55),
    trip_distance=19.32,
)


class NYTaxiRideSilver(BaseTestData):
    pep_pickup_date_txt: str
    DOLocationID: int
    PULocationID: int
    RatecodeID: int
    VendorID: int
    congestion_surcharge: float
    extra: float
    fare_amount: float
    improvement_surcharge: float = Field(default=0.3)
    mta_tax: float = Field(default=0.5)
    passenger_count: int
    payment_type: int
    store_and_fwd_flag: str = Field(default="N")
    tip_amount: float
    tolls_amount: float
    total_amount: float
    tpep_dropoff_datetime: datetime
    tpep_pickup_datetime: datetime
    trip_distance: float
    is_short_ride: bool
    row_processing_ts: datetime = Field(alias="_row_processing_ts")


pep_pickup_date_txt_field = StructField("pep_pickup_date_txt", StringType(), True)
is_short_ride_field = StructField("is_short_ride", BooleanType(), True)
row_processing_ts_field = StructField("_row_processing_ts", TimestampType(), True)
ny_taxi_silver_schema = StructType(
    [pep_pickup_date_txt_field] + ny_taxi_bronze_schema.fields + [is_short_ride_field, row_processing_ts_field]
)

ny_taxi_silver_sample_ride = NYTaxiRideSilver(
    **ny_taxi_bronze_sample_ride.dict(by_alias=True),
    is_short_ride=False,
    pep_pickup_date_txt=pep_pickup_date_txt,
    _row_processing_ts=processing_ts,
)


def get_ny_taxi_bronze_schema_w_tech_cols() -> StructType:
    processing_ts_field = StructField("_processing_ts", TimestampType(), True)
    return StructType([pep_pickup_date_txt_field] + ny_taxi_bronze_schema.fields + [processing_ts_field])


def get_ny_taxi_bronze_sample_msg_w_tech_cols() -> Dict:
    ny_taxi_bronze_sample_ride_dict = ny_taxi_bronze_sample_ride.dict(by_alias=True)
    ny_taxi_bronze_sample_ride_dict["_processing_ts"] = processing_ts
    ny_taxi_bronze_sample_ride_dict["pep_pickup_date_txt"] = pep_pickup_date_txt
    return ny_taxi_bronze_sample_ride_dict
