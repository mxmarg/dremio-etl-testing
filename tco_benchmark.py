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


def generate_dummy_data(api: dremio_api.DremioAPI, run_id, num_days) -> list[str]:
    queries = []
    with open("00_generate_dummy_data.sql", 'r') as file:
        query = file.read()
  
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        q = query.replace("<INSERT_DATE_HERE>", date_str)
        job_name = f"generate_dummy_data - {date_str}"
        queries.append({"job_name": job_name, "sql": q})
    return queries

def create_raw_table(api: dremio_api.DremioAPI, run_id):
    with open("file_ingest/raw_create.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    print(f"create_raw_table")
    job_id = api.post_sql_query(query)

def create_processed_table(api: dremio_api.DremioAPI, run_id):
    with open("file_ingest/processed_create.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    print(f"create_processed_table")
    job_id = api.post_sql_query(query)

def create_agg_view(api: dremio_api.DremioAPI, run_id):
    with open("05_aggregates_view.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    print(f"create_agg_view")
    job_id = api.post_sql_query(query)

def promote_parquet_files(api: dremio_api.DremioAPI, run_id, num_days, num_files) -> list[str]:
    queries = []
    query = 'ALTER TABLE sizingtest."<INSERT_RUN_ID_HERE>"."raw_data"."<INSERT_DATE_HERE>"."0_0_<INSERT_NUM_HERE>.parquet" REFRESH METADATA AUTO PROMOTION'
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for j in range(0, num_files+1):
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_NUM_HERE>", str(j))
            job_name = (f"promote_parquet_files - {date_str} 0_0_{j}.parquet")
            queries.append({"job_name": job_name, "sql": q})
    return queries

def file_to_temp(api: dremio_api.DremioAPI, run_id, num_days, num_files):
    queries = []
    with open("file_ingest/01_temp_create.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for j in range(0, num_files+1):
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_NUM_HERE>", str(j))
            job_name = f"file_to_temp - {date_str} 0_0_{j}.parquet"
            queries.append({"job_name": job_name, "sql": q})
    return queries

def temp_to_raw(api: dremio_api.DremioAPI, run_id, num_days, num_files):
    queries = []
    with open("file_ingest/02_raw_insert.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for j in range(0, num_files+1):
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_NUM_HERE>", str(j))
            job_name = f"temp_to_raw - {date_str} 0_0_{j}"
            queries.append({"job_name": job_name, "sql": q})
    return queries

def raw_to_processed_insert(api: dremio_api.DremioAPI, run_id, num_days):
    queries = []
    # input_range_VARCHAR = ["C_H101", "C_H110"]
    # input_range_INTEGER = ["C_H111", "C_H120"]
    # input_range_FLOAT = ["C_H121", "C_H130"]
    with open("file_ingest/03_processed_insert.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    for i in range(1, num_days+1):
        date_str = "2000-01-" + str(i).rjust(2, "0")
        for h in range(0, 24):
            q = query.replace("<INSERT_DATE_HERE>", date_str).replace("<INSERT_HOUR_HERE>", str(h))
            job_name = f"raw_to_processed - {date_str} {h}"
            queries.append({"job_name": job_name, "sql": q})
    return queries

def raw_to_processed_merge(api: dremio_api.DremioAPI, run_id):
    # input_range_VARCHAR = ["C_H101", "C_H110"]
    # input_range_INTEGER = ["C_H111", "C_H120"]
    # input_range_FLOAT = ["C_H121", "C_H130"]
    with open("file_ingest/04_processed_merge_update.sql", 'r') as file:
        query = file.read()
    query = query.replace("<INSERT_RUN_ID_HERE>", run_id)
    print(f"raw_to_processed_merge - MERGE .. UPDATE")
    job_id = api.post_sql_query(query)
    api.get_query_info(job_id)

    query = query.replace("WHEN MATCHED THEN UPDATE SET", "WHEN NOT MATCHED THEN INSERT")
    print(f"raw_to_processed_merge - MERGE .. INSERT")
    job_id = api.post_sql_query(query)
    api.get_query_info(job_id)
