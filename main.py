import concurrent.futures
from datetime import datetime
import os
import time
import tco_benchmark
from dremio_api import DremioAPI
from configparser import ConfigParser


def run_job(api: DremioAPI, sql: str):
    job_id = api.post_sql_query(sql)
    job_state = api.get_query_info(job_id)
    return job_state

def run_sequential_jobs(api: DremioAPI, queries: list[dict]):
    start_time = time.time()
    start_time_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n--- {start_time_str} Submitting {len(queries)} sequential jobs ---")

    for q in queries:
        job_name = q["job_name"]
        print(f"Submitted {job_name}")
        job_state = run_job(api, q["sql"])
        if job_state != 'COMPLETED':
            print(job_state)

    end_time = time.time()
    end_time_str = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
    duration = end_time - start_time
    print(f"--- {end_time_str} All {len(queries)} sequential jobs Ran in {duration:.2f} seconds ---")

def run_parallel_jobs(api: DremioAPI, queries: list[dict], max_workers=5):
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        
        num_parallel_jobs = len(queries)
        start_time_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- {start_time_str} Submitting {num_parallel_jobs} Parallel Jobs ---")
        parallel_futures = []
        for q in queries:
            future = executor.submit(run_job, api, q["sql"])
            parallel_futures.append(future)
        print(f"     ... Waiting for all {num_parallel_jobs} jobs to complete ...")

        done, not_done = concurrent.futures.wait(parallel_futures, return_when=concurrent.futures.ALL_COMPLETED)
        parallel_results = []
        for future in done:
            try:
                parallel_results.append(future.result())
            except Exception as e:
                print(f"A parallel job raised an exception: {e}")
                parallel_results.append(f"Error: {e}")
        
        end_time = time.time()
        end_time_str = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        print(f"--- {end_time_str} All {num_parallel_jobs} Parallel Jobs Ran in {duration:.2f} seconds ---")
        for res in parallel_results:
            if res != 'COMPLETED':
                print(f" - {res}")


if __name__ == "__main__":
    # Authenticate via REST API
    filepath_dir = os.path.dirname(os.path.abspath(__file__))
    parser = ConfigParser()
    _ = parser.read(os.path.join(filepath_dir, "config.cfg"))
    user = parser.get('Authentication', 'user')
    password = parser.get('Authentication', 'password')
    dremio_url = parser.get('Authentication', 'dremio_endpoint')

    api = DremioAPI(user, password, dremio_url, timeout=60)
    api_prio = DremioAPI('priority_user', password, dremio_url, timeout=60)

    RUN_ID = tco_benchmark.generate_timestamped_run_id()
    NUM_DAYS = 1
    NUM_FILES_PER_DAY = 10
    MAX_PARALLEL_JOBS_SUBMITTED = 10

    print(f"Run ID: {RUN_ID}")
    print(f"NUM_DAYS: {NUM_DAYS}")
    print(f"NUM_FILES_PER_DAY: est. {NUM_FILES_PER_DAY}")

    ### Preparation ###
    # print(f"\n### Preparation - Generate Dummy Data Files ###")
    # dummy_data_queries = tco_benchmark.generate_dummy_data(api, RUN_ID, NUM_DAYS)
    # run_parallel_jobs(api, queries=dummy_data_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    tco_benchmark.create_raw_table(api, RUN_ID)
    tco_benchmark.create_processed_table(api, RUN_ID)
    tco_benchmark.create_agg_view(api, RUN_ID)

    print(f"\n### STEP 1 - Promote Parquet files ###")
    promote_queries = tco_benchmark.promote_parquet_files(RUN_ID, NUM_DAYS, NUM_FILES_PER_DAY)
    run_parallel_jobs(api, queries=promote_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 2 - Ingest files into temp tables ###")
    file_ingest_queries = tco_benchmark.file_to_temp(RUN_ID, NUM_DAYS, NUM_FILES_PER_DAY)
    run_parallel_jobs(api, queries=file_ingest_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 3 - Ingest temp tables into raw table ###")
    raw_ingest_queries = tco_benchmark.temp_to_raw(RUN_ID, NUM_DAYS, NUM_FILES_PER_DAY)
    run_parallel_jobs(api, queries=raw_ingest_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 4 - Ingest raw table into processed table ###")
    processed_insert_queries = tco_benchmark.raw_to_processed_insert(RUN_ID, NUM_DAYS)
    run_sequential_jobs(api_prio, queries=processed_insert_queries)

    print(f"\n### STEP 5 - Merge raw into processed table across partitions ###")
    processed_merge_queries = tco_benchmark.raw_to_processed_merge(RUN_ID, NUM_DAYS)
    run_sequential_jobs(api_prio, queries=processed_merge_queries)

    print(f"\n### STEP 6 - Refresh raw reflection on aggregated view on processed table (Should have been happening incrementally in the background...) ###\n")

    print(f"\n### STEP 8 - Optimize raw table ###")
    optimize_raw_queries = tco_benchmark.optimize_raw(RUN_ID, NUM_DAYS)
    run_parallel_jobs(api, queries=optimize_raw_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 9 - Optimize processed table ###")
    optimize_processed_queries = tco_benchmark.optimize_processed(RUN_ID, NUM_DAYS)
    run_parallel_jobs(api, queries=optimize_processed_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 10 - Expiring snapshots via vacuum ###")
    vacuum_queries = tco_benchmark.vacuum_tables(RUN_ID)
    run_parallel_jobs(api, queries=vacuum_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    # query = 'DROP TABLE sizingtest.raw_data."<INSERT_DATE_HERE>"'
    # query = 'ALTER TABLE sizingtest.raw_data."<INSERT_DATE_HERE>" REFRESH METADATA AUTO PROMOTION'
    # query = 'DROP TABLE sizingtest."temp"."0_0_<INSERT_NUM_HERE>"' # <10s
    # query = 'ALTER TABLE sizingtest."raw_data"."10mb_30char"."0_0_<INSERT_NUM_HERE>.parquet" REFRESH METADATA AUTO PROMOTION' # 10s