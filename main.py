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
    job_states = ""
    for q in queries:
        # job_name = q["job_name"]
        # print(f"Submitted {job_name}")
        job_state = run_job(api, q["sql"])
        if job_state != 'COMPLETED':
            job_states += job_state + "\n"
            print(job_state)
    if job_states == "":
        job_states = 'COMPLETED'
    return job_states

def run_parallel_jobs(api: DremioAPI, query_sets: list[list[dict]], max_workers=5):
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        
        num_parallel_sets = len(query_sets)
        start_time_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- {start_time_str} Submitting {num_parallel_sets} parallel query sets ---")
        parallel_futures = []
        for qs in query_sets:
            future = executor.submit(run_sequential_jobs, api, qs)
            parallel_futures.append(future)
        print(f"     ... Waiting for all {num_parallel_sets} query sets to complete ...")

        done, not_done = concurrent.futures.wait(parallel_futures, return_when=concurrent.futures.ALL_COMPLETED)
        parallel_results = []
        for future in done:
            try:
                parallel_results.append(future.result())
            except Exception as e:
                print(f"A query set raised an exception: {e}")
                parallel_results.append(f"Error: {e}")
        
        end_time = time.time()
        end_time_str = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        print(f"--- {end_time_str} All {num_parallel_sets} parallel query sets ran in {duration:.2f} seconds ---")
        for res in parallel_results:
            if res != 'COMPLETED':
                print(f" - {res}")


def multi_tenant_run(api, num_tenants: int):
    tenant_ids = [f"TENANT_{i}" for i in range(num_tenants)]

    print(f"\n### STEP 0 - Preparing table, view, and reflection definitions ###")
    drop_table_queries = []
    drop_view_queries = []
    create_table_queries = []
    create_view_queries = []
    create_reflection_queries = []

    for tenant_id in tenant_ids:
        drop_table_queries += tco_benchmark.drop_tables(run_id=tenant_id, num_days=NUM_DAYS, num_files=NUM_FILES_PER_DAY)
        drop_view_queries += tco_benchmark.drop_views(run_id=tenant_id)
        create_table_queries += tco_benchmark.create_tables(run_id=tenant_id)
        create_view_queries += tco_benchmark.create_agg_views(run_id=tenant_id)
        create_reflection_queries += tco_benchmark.create_agg_reflections(run_id=tenant_id)

    run_parallel_jobs(api, drop_table_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)
    run_parallel_jobs(api, drop_view_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)
    run_parallel_jobs(api, create_table_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)
    run_parallel_jobs(api, create_view_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)
    run_parallel_jobs(api, create_reflection_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    # print(f"\n### STEP 1 - Promote Parquet files ###")
    # promote_queries = tco_benchmark.promote_parquet_files(RUN_ID, NUM_DAYS, NUM_FILES_PER_DAY)
    # run_parallel_jobs(api, queries=promote_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 2 - Ingest files into temp tables ###")
    file_ingest_queries = []
    for tenant_id in tenant_ids:
        file_ingest_queries += tco_benchmark.file_to_temp(tenant_id, NUM_DAYS, NUM_FILES_PER_DAY)
    run_parallel_jobs(api, file_ingest_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 3 - Ingest temp tables into raw table ###")
    raw_ingest_queries = []
    for tenant_id in tenant_ids:
        query_set = tco_benchmark.temp_to_raw(tenant_id, NUM_DAYS, NUM_FILES_PER_DAY)
        raw_ingest_queries += query_set
    run_parallel_jobs(api, raw_ingest_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 4 - Ingest raw table into processed table ###")
    processed_insert_queries = []
    for tenant_id in tenant_ids:
        query_set = tco_benchmark.raw_to_processed_insert(tenant_id, NUM_DAYS)
        processed_insert_queries += query_set
    run_parallel_jobs(api, processed_insert_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 5 - Merge raw into processed table across partitions ###")
    processed_merge_queries = []
    for tenant_id in tenant_ids:
        query_set = tco_benchmark.raw_to_processed_merge(tenant_id, NUM_DAYS)
        processed_merge_queries += query_set
    run_parallel_jobs(api, processed_merge_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 6 - Refresh raw reflection on aggregated view on processed table (Should have been happening incrementally in the background...) ###\n")

    print(f"\n### STEP 8 - Optimize raw and processed table ###")

    optimize_queries = []
    for tenant_id in tenant_ids:
        # optimize_queries += tco_benchmark.optimize_raw(tenant_id, NUM_DAYS)
        optimize_queries += tco_benchmark.optimize_processed(tenant_id, NUM_DAYS)
    run_parallel_jobs(api, optimize_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    print(f"\n### STEP 9 - Expiring snapshots via vacuum ###")
    vacuum_queries = []
    for tenant_id in tenant_ids:
        vacuum_queries += tco_benchmark.vacuum_tables(RUN_ID)
    run_parallel_jobs(api, vacuum_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

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
    MAX_PARALLEL_JOBS_SUBMITTED = 20

    print(f"Run ID: {RUN_ID}")
    print(f"NUM_DAYS: {NUM_DAYS}")
    print(f"NUM_FILES_PER_DAY: est. {NUM_FILES_PER_DAY}")

    # ### Preparation ###
    # print(f"\n### Preparation - Generate Dummy Data Files ###")
    # dummy_data_queries = tco_benchmark.generate_dummy_data(RUN_ID, NUM_DAYS)
    # run_parallel_jobs(api, dummy_data_queries, max_workers=MAX_PARALLEL_JOBS_SUBMITTED)

    NUM_TENANTS = 10
    multi_tenant_run(api, NUM_TENANTS)