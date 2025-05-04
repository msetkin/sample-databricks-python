import json
import os
import uuid
from typing import Dict, List

from pyspark.sql import Row, SparkSession

from python_helper.common import DBUtilities


def drop_table(table_row: Dict, spark: SparkSession) -> None:
    db_utils = DBUtilities.get_dbutils(spark)
    tbl_type = table_row["tbl_type"]
    table = table_row["table"]
    schema = table_row["schema"]
    location = table_row["location"]

    if tbl_type == "VIEW":
        query = f"drop view `{schema}`.{table}"
        spark.sql(query)
    else:
        query = f"drop table `{schema}`.{table}"
        spark.sql(query)

        if tbl_type == "EXTERNAL":
            db_utils.fs.rm(location, True)


def get_tables_by_github_run_id(db_schema: str, github_run_id: str, spark: SparkSession) -> List[Row]:
    if not github_run_id:
        raise ValueError(f"the value of github_run_id cannot be None/empty, got: {github_run_id}")

    tables_df = spark.sql(f"show table extended in `{db_schema}` like '*{github_run_id.lower()}*';")
    return tables_df.select("database", "tableName", "Information").collect()


def parse_table_info(tables: List[Row]) -> List[Dict]:
    location_indicator = "Location: "
    tbl_type_indicator = "Type: "

    parsed_table_data = []
    for table in tables:
        tbl_type = ""
        location = ""

        table_info = table[2]
        for info_item in table_info.split("\n"):
            if info_item.startswith(tbl_type_indicator):
                tbl_type = info_item.split(": ")[1]
            elif info_item.startswith(location_indicator):
                location = info_item.split(": ")[1]
        parsed_table_data.append({"schema": table[0], "table": table[1], "tbl_type": tbl_type, "location": location})

    table_data_str = "\n".join([x["table"] for x in parsed_table_data])
    print(f"""Tables retrieved:\n{table_data_str}""")
    return parsed_table_data


def drop_tables_from_list(tables: List[Dict], spark: SparkSession) -> None:
    for i, table in enumerate(tables, 1):
        print(f"step {i}/{len(tables)}: dropping table {table['schema']}.{table['table']}... ", sep="")
        drop_table(table, spark)
        print("Done!")


def clean_up_environment_from_tables(db_schema: str, github_run_id: str, spark: SparkSession):
    drop_tables_from_list(parse_table_info(get_tables_by_github_run_id(db_schema, github_run_id, spark)), spark)


def save_dict_to_random_path(data_dict):
    random_dir = os.path.join(os.getcwd(), str(uuid.uuid4()))
    os.makedirs(random_dir, exist_ok=True)
    fixed_filename = "ny_taxi.json"
    file_path = os.path.join(random_dir, fixed_filename)
    with open(file_path, "w") as json_file:
        json.dump(data_dict, json_file, ensure_ascii=False, indent=4)

    return file_path
