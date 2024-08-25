import requests

url = "http://localhost:8080/api/v1/dags/bronze_igdb/dagRuns"
self.headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
        }
        self.base_url = 'https://api.igdb.com/v4/{sufix}'
        self.delay = delay
        self.delay_timestamp = int((datetime.datetime.now() - datetime.timedelta(days=delay)).timestamp())

    def get_data(self, sufix, params={}):

        url = self.base_url.format(sufix=sufix)
        data = requests.get(url, headers=self.headers, params=params)
        return data.json()