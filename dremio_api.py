import pandas as pd
import requests
import time

class DremioAPI:

    def __init__(self, dremio_user, dremio_password, dremio_url, timeout=30, verify=True):
        self.dremio_user = dremio_user
        self.dremio_url = dremio_url
        self.timeout = timeout
        self.verify = verify
        self.auth_token = self.get_auth_token(dremio_user, dremio_password, dremio_url)
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': self.auth_token
        }

    def get_auth_token(self, dremio_user, dremio_password, dremio_endpoint):
        headers = {"Content-Type": "application/json"}
        payload: str = '{"userName": "' + dremio_user + '","password": "' + dremio_password + '"}'
        payload = payload.encode(encoding='utf-8')
        response = requests.request("POST", dremio_endpoint + "/apiv2/login", data=payload, headers=headers,
                                    timeout=self.timeout, verify=self.verify)
        if response.status_code != 200:
            print("Authentication Error " + str(response.status_code) + ' ' + response.text)
            raise RuntimeError("Authentication error.")
        return '_dremio' + response.json()['token']

    def create_space(self, space_name: list):
        payload: str = '{"entityType": "space", "name": "' + space_name + '"}'
        payload = payload.encode(encoding='utf-8')
        response = requests.post(
                self.dremio_url + '/api/v3/catalog/',
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify,
                data=payload
        )
        # if response.status_code != 200:
        #     print("Error creating space: " + str(response.status_code) + ' ' + response.text)

    def get_query_info(self, job_id: str):
        while True:
            response = requests.get(
                self.dremio_url + '/api/v3/job/' + job_id,
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify
            )
            data = response.json()
            job_state = data['jobState']
            if job_state == 'COMPLETED':
                break
            elif job_state in {"CANCELED", "FAILED"}:
                job_state = job_state + " - " + data.get("errorMessage", "")
                break
            else:
                time.sleep(1)
        return job_state

    def post_sql_query(self, sql: str):
        response = requests.post(
            self.dremio_url + '/api/v3/sql',
            headers=self.headers,
            json={"sql": sql},
            timeout=self.timeout,
            verify=self.verify
        )
        job_id = response.json()['id']

        return job_id

    def get_query_data(self, job_id: str, limit=500) -> dict:
        job_state = self.get_query_info(job_id)

        if job_state == 'COMPLETED':
            rows = []
            new_rows = ['init_dummy']
            current_offset = 0
            job_results_json = {}
            while len(new_rows) > 0:
                page = 'offset=' + str(current_offset) + '&limit=' + str(limit)
                print("Paging " + page)
                job_results = requests.get(
                    self.dremio_url + '/apiv2/job/' + job_id + '/data?' + page,
                    # '/api/v3/job/' + job_id + '/results
                    headers=self.headers,
                    timeout=self.timeout,
                    verify=self.verify
                )
                if job_results.status_code != 200:
                    Exception(f'Error - {job_results.text}')
                job_results_json = job_results.json()
                new_rows = job_results_json['rows']
                current_offset += len(new_rows)
                rows.extend(new_rows)
            columns = job_results_json.get('columns', [])
            return {"rows": rows, "columns": columns}
        else:
            raise Exception(f'Query data could not be retrieved - Incorrect Job State: {job_state}')

    def query_result_data_to_df(self, result_data: dict) -> pd.DataFrame:
        cols = []
        for col in result_data['columns']:
            cols.append(col['name'])

        data = []
        for row in result_data['rows']:
            row_data = []
            for value in row['row']:
                row_data.append(value['v'])
            data.append(row_data)

        return pd.DataFrame(data, columns=cols)
