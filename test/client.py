import requests
from validators import *
from exceptions import APIError


class DatabaseClient:
    def __init__(self, base_url="http://localhost:8080"):
        self.base_url = base_url

    def _handle(self, resp):
        validate_status(resp)
        return resp.json()

    # ------------------ TABLE ------------------

    def create_table(self, name):
        resp = requests.post(f"{self.base_url}/api/tables", json={"name": name})
        data = self._handle(resp)
        validate_create_table(data)
        return data

    def list_tables(self):
        resp = requests.get(f"{self.base_url}/api/tables")
        return self._handle(resp)

    def get_table_id(self, name):
        resp = requests.get(f"{self.base_url}/api/tables/by-name/{name}")
        return self._handle(resp)

    def drop_table(self, table_id):
        resp = requests.delete(f"{self.base_url}/api/tables/{table_id}")
        return self._handle(resp)

    def get_schema(self, table_id):
        resp = requests.get(f"{self.base_url}/api/tables/{table_id}/schema")
        data = self._handle(resp)
        validate_schema(data)
        return data

    def get_size(self, table_id):
        resp = requests.get(f"{self.base_url}/api/tables/{table_id}/size")
        return self._handle(resp)

    # ------------------ COLUMN ------------------

    def create_column(self, table_id, payload):
        resp = requests.post(
            f"{self.base_url}/api/tables/{table_id}/columns",
            json=payload,
        )
        data = self._handle(resp)
        validate_create_column(data)
        return data

    def drop_column(self, table_id, column_id):
        resp = requests.delete(
            f"{self.base_url}/api/tables/{table_id}/columns/{column_id}"
        )
        return self._handle(resp)

    def create_index(self, table_id, column_id):
        resp = requests.post(
            f"{self.base_url}/api/tables/{table_id}/columns/{column_id}/index"
        )
        return self._handle(resp)

    def drop_index(self, table_id, column_id):
        resp = requests.delete(
            f"{self.base_url}/api/tables/{table_id}/columns/{column_id}/index"
        )
        return self._handle(resp)

    # ------------------ ROW ------------------

    def insert_rows(self, table_id, payload):
        resp = requests.post(
            f"{self.base_url}/api/tables/{table_id}/rows",
            json=payload,
        )
        data = self._handle(resp)
        validate_insert(data)
        return data

    def get_rows(self, table_id, row_ids):
        resp = requests.post(
            f"{self.base_url}/api/tables/{table_id}/rows/get",
            json={"row_ids": row_ids},
        )
        return self._handle(resp)

    def delete_rows(self, table_id, payload):
        resp = requests.delete(
            f"{self.base_url}/api/tables/{table_id}/rows",
            json=payload,
        )
        return self._handle(resp)

    def update_rows(self, table_id, payload):
        resp = requests.put(
            f"{self.base_url}/api/tables/{table_id}/rows",
            json=payload,
        )
        return self._handle(resp)

    def search(self, table_id, payload):
        resp = requests.post(
            f"{self.base_url}/api/tables/{table_id}/search",
            json=payload,
        )
        data = self._handle(resp)
        validate_search(data)
        return data

    # ------------------ HEALTH ------------------

    def health(self):
        resp = requests.get(f"{self.base_url}/api/health")
        return self._handle(resp)
