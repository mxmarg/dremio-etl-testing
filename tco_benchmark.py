import datetime
import dremio_api

def generate_timestamped_run_id(prefix="run"):
    """
    Generates a run ID with a timestamp.

    Args:
        prefix: An optional string to prepend to the timestamp.

    Returns:
        A string representing the timestamped run ID.
    """
    now = datetime.datetime.now()
    # Format: YYYYMMDD_HHMMSS (YearMonthDay_HourMinuteSecond)
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp_str}"    


def generate_dummy_data(run_id, num_days) -> list[str]:
    queries = []
    with open("00_generate_dummy_data.sql", 'r') as file:
        query = file.read()
  
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        q = query.replace("<INSERT_DATE_HERE>", date_str)
        job_name = f"generate_dummy_data - {date_str}"
        queries.append([{"job_name": job_name, "sql": q}])
    return queries

def create_tables(run_id):
    queries = []
    with open("file_ingest/raw_create.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    queries.append([{"job_name": "create_raw_table", "sql": query}])

    with open("file_ingest/processed_create.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    queries.append([{"job_name": "create_processed_table", "sql": query}])
    return queries

def create_agg_views(run_id):
    queries = []
    with open("05_aggregates_view.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    queries.append([{"job_name": "create_agg_view", "sql": query}])
    return queries

def create_agg_reflections(run_id):
    queries = []
    with open("06_aggregates_reflection.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    queries.append([{"job_name": "create_agg_reflection", "sql": query}])
    return queries

def promote_parquet_files(run_id, num_days, num_files) -> list[str]:
    queries = []
    query = 'ALTER TABLE <INSERT_SOURCE_PATH_HERE>."raw_data"."<INSERT_DATE_HERE>"."0_0_<INSERT_NUM_HERE>.parquet" REFRESH METADATA AUTO PROMOTION'
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for j in range(0, num_files):
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_NUM_HERE>", str(j))
            job_name = (f"promote_parquet_files - {date_str} 0_0_{j}.parquet")
            queries.append([{"job_name": job_name, "sql": q}])
    return queries

def file_to_temp(run_id, num_days, num_files):
    queries = []
    with open("file_ingest/01_temp_create.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for j in range(0, num_files):
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_NUM_HERE>", str(j))
            job_name = f"file_to_temp - {date_str} 0_0_{j}.parquet"
            queries.append([{"job_name": job_name, "sql": q}])
    return queries

def temp_to_raw(run_id, num_days, num_files):
    queries = []
    with open("file_ingest/02_raw_insert.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for j in range(0, num_files):
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_NUM_HERE>", str(j))
            job_name = f"temp_to_raw - {date_str} 0_0_{j}"
            queries.append({"job_name": job_name, "sql": q})
    query_set = [queries]
    return query_set

def raw_to_processed_insert(run_id, num_days):
    queries = []
    # input_range_VARCHAR = ["C_H101", "C_H110"]
    # input_range_INTEGER = ["C_H111", "C_H120"]
    # input_range_FLOAT = ["C_H121", "C_H130"]
    with open("file_ingest/03_processed_merge_insert.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    hour_step = 1
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for h in range(0, 24, hour_step):
            hour_range = f'"hour" >= {h} AND "hour" < {h+hour_step}'
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_HOUR_RANGE_HERE>", hour_range)
            job_name = f"raw_to_processed_insert - {date_str} {hour_range}"
            queries.append({"job_name": job_name, "sql": q})
    query_set = [queries]
    return query_set

def raw_to_processed_merge(run_id, num_days):
    queries = []
    # input_range_VARCHAR = ["C_H101", "C_H110"]
    # input_range_INTEGER = ["C_H111", "C_H120"]
    # input_range_FLOAT = ["C_H121", "C_H130"]
    with open("file_ingest/04_processed_merge_update_insert.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    hour_step = 3
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for h in range(0, 3, hour_step):
            hour_range = f'"hour" >= {h} AND "hour" < {h+hour_step}'
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_HOUR_RANGE_HERE>", hour_range)
            job_name = f"raw_to_processed_merge - {date_str} {hour_range}"
            queries.append({"job_name": job_name, "sql": q})
    query_set = [queries]
    return query_set

def optimize_raw(run_id, num_days):
    queries = []
    with open("file_ingest/08_optimize_raw.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    hour_step = 1
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for h in range(0, 24, hour_step):
            hour_range = f'"hour" >= {h} AND "hour" < {h+hour_step}'
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_HOUR_RANGE_HERE>", hour_range)
            job_name = f"optimize_raw - {date_str} {hour_range}"
            queries.append([{"job_name": job_name, "sql": q}])
    return queries

def optimize_processed(run_id, num_days):
    queries = []
    with open("file_ingest/09_optimize_processed.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    hour_step = 12
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for h in range(0, 24, hour_step):
            hour_range = f'"hour" >= {h} AND "hour" < {h+hour_step}'
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_HOUR_RANGE_HERE>", hour_range)
            job_name = f"optimize_processed - {date_str} {hour_range}"
            queries.append([{"job_name": job_name, "sql": q}])
    return queries

def vacuum_tables(run_id):
    queries = [
        [{"job_name": "vacuum_raw_table",       "sql": f'VACUUM TABLE <INSERT_SOURCE_PATH_HERE>."{run_id}".raw_table EXPIRE SNAPSHOTS retain_last 10'}],
        [{"job_name": "vacuum_processed_table", "sql": f'VACUUM TABLE <INSERT_SOURCE_PATH_HERE>."{run_id}".processed_table EXPIRE SNAPSHOTS retain_last 10'}],
    ]
    return queries

def drop_tables(run_id, num_days, num_files):
    queries = [
        [{"job_name": "drop_raw_table",       "sql": f'DROP TABLE IF EXISTS <INSERT_SOURCE_PATH_HERE>."{run_id}".raw_table'}],
        [{"job_name": "drop_processed_table", "sql": f'DROP TABLE IF EXISTS <INSERT_SOURCE_PATH_HERE>."{run_id}".processed_table'}],
    ]
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for n in range(0, num_files):
            queries.append([{"job_name": f"drop_temp_table_{date_str}_{n}", "sql": f'DROP TABLE IF EXISTS <INSERT_SOURCE_PATH_HERE>."{run_id}"."temp"."{date_str}_0_0_{n}"'}])
    return queries

def drop_views(run_id):
    queries = [
        [{"job_name": "drop_view",       "sql": f'DROP VIEW IF EXISTS sizingtest_space."aggregates_{run_id}"'}]
    ]
    return queries

def alter_tables(run_id):
    query = f'''
ALTER TABLE <INSERT_SOURCE_PATH_HERE>."{run_id}".processed_table SET TBLPROPERTIES (
        'write.delete.mode'='merge-on-read',
        'write.update.mode'='merge-on-read',
        'write.merge.mode'='merge-on-read',
        'write.parquet.compression-codec'='zstd'
)
'''
    return [[{"job_name": "alter_processed_table", "sql": query}]]