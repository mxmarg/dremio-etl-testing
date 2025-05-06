import concurrent.futures
import os
import time
import tco_benchmark
from dremio_api import DremioAPI
from configparser import ConfigParser


def run_job(api: DremioAPI, sql: str):
    job_id = api.post_sql_query(sql)
    job_state = api.get_query_info(job_id)
    return job_state


def run_parallel_jobs(api: DremioAPI, queries: list[dict], max_workers=30):

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        
        num_parallel_jobs = len(queries)
        print(f"\n--- Submitting {num_parallel_jobs} Parallel Jobs ---")
        parallel_futures = []
        for q in queries:
            future = executor.submit(run_job, api, q["sql"])
            parallel_futures.append(future)
        print(f"\n--- Waiting for all {num_parallel_jobs} Parallel Jobs to Complete ---")

        done, not_done = concurrent.futures.wait(parallel_futures, return_when=concurrent.futures.ALL_COMPLETED)
        parallel_results = []
        for future in done:
            try:
                parallel_results.append(future.result())
            except Exception as e:
                print(f"A parallel job raised an exception: {e}")
                parallel_results.append(f"Error: {e}")
        
        print(f"--- All {num_parallel_jobs} Parallel Jobs Ran ---")
        print("The following jobs failed:")
        for res in parallel_results:
            if res != 'COMPLETED':
                print(f"  - {res}")


if __name__ == "__main__":
    # Authenticate via REST API
    filepath_dir = os.path.dirname(os.path.abspath(__file__))
    parser = ConfigParser()
    _ = parser.read(os.path.join(filepath_dir, "config.cfg"))
    user = parser.get('Authentication', 'user')
    password = parser.get('Authentication', 'password')
    dremio_url = parser.get('Authentication', 'dremio_endpoint')

    api = DremioAPI(user, password, dremio_url)

    RUN_ID = tco_benchmark.generate_timestamped_run_id()
    NUM_DAYS = 3
    NUM_FILES_PER_DAY = 10

    print(f"Run ID: {RUN_ID}")
    print(f"NUM_DAYS: {NUM_DAYS}")
    print(f"NUM_FILES_PER_DAY: est. {NUM_FILES_PER_DAY}")

    ### Preparation ###
    print(f"\n### Preparation - Generate Dummy Data Files ###")
    dummy_data_queries = tco_benchmark.generate_dummy_data(api, RUN_ID, NUM_DAYS)
    run_parallel_jobs(api, queries=dummy_data_queries)

    tco_benchmark.create_raw_table(api, RUN_ID)
    tco_benchmark.create_processed_table(api, RUN_ID)
    tco_benchmark.create_agg_view(api, RUN_ID)

    print(f"\n### STEP 1 - Promote Parquet files ###")
    promote_queries = tco_benchmark.promote_parquet_files(api, RUN_ID, NUM_DAYS, NUM_FILES_PER_DAY)
    run_parallel_jobs(api, queries=promote_queries)

    print(f"\n### STEP 2 - Ingest files into temp tables ###")
    file_ingest_queries = tco_benchmark.file_to_temp(api, RUN_ID, NUM_DAYS, NUM_FILES_PER_DAY)
    run_parallel_jobs(api, queries=file_ingest_queries)

    print(f"\n### STEP 3 - Ingest temp tables into raw table ###")
    raw_ingest_queries = tco_benchmark.temp_to_raw(api, RUN_ID, NUM_DAYS, NUM_FILES_PER_DAY)
    run_parallel_jobs(api, queries=raw_ingest_queries)

    print(f"\n### STEP 4 - Ingest raw table into processed table ###")
    processed_insert_queries = tco_benchmark.raw_to_processed_insert(api, RUN_ID, NUM_DAYS)
    run_parallel_jobs(api, queries=processed_insert_queries)

    ### PART 3b ###
    # tco_benchmark.raw_to_processed_merge(api, RUN_ID)

    # query = 'DROP TABLE sizingtest.raw_data."<INSERT_DATE_HERE>"'
    # query = 'ALTER TABLE sizingtest.raw_data."<INSERT_DATE_HERE>" REFRESH METADATA AUTO PROMOTION'
    # query = 'DROP TABLE sizingtest."temp"."0_0_<INSERT_NUM_HERE>"' # <10s
    # query = 'ALTER TABLE sizingtest."raw_data"."10mb_30char"."0_0_<INSERT_NUM_HERE>.parquet" REFRESH METADATA AUTO PROMOTION' # 10s