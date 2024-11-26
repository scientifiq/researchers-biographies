from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions
import json
import time

class BigQueryAPI:

    def __init__(self, dataset_id):
        key_path = "keys/com-sci-2-b87c4583760e.json"
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        self.dataset_id = dataset_id

    def get_empty_researchers(self, table, limit, greater_than="", less_or_equal_than=""):
        external_full_table_id = f"{self.client.project}.{self.dataset_id}.{table}"

        extra = ""
        if greater_than:
            extra += f" AND res_id > '{greater_than}'"
        if less_or_equal_than:
            extra += f" AND res_id <= '{less_or_equal_than}'"

        query = f"SELECT * FROM `{external_full_table_id}` WHERE res_bio IS NULL {extra} ORDER BY res_total_pubs DESC LIMIT {limit}"
        query_job = self.client.query(query)
        results = query_job.result()

        data = []
        for row in results:
            data.append(dict(row.items()))

        return data
    
    def update_researchers_in_bulk(self, target_table, final_table, updates, greater_than="", less_or_equal_than=""):
        print(f"Starting bulk update for {len(updates)} researchers")

        target_table_id = f"{self.client.project}.{self.dataset_id}.{target_table}"
        final_table_id = f"{self.client.project}.{self.dataset_id}.{final_table}"
        temp_table_id = f"{self.client.project}.{self.dataset_id}.temp_biographies_updates_{greater_than}_{less_or_equal_than}"

        self.create_temp_table(temp_table_id)
        self.load_updates_into_temp_table(temp_table_id, updates)
        self.merge_updates(target_table_id, temp_table_id)
        self.merge_with_final(temp_table_id, final_table_id)
        self.drop_temp_table(temp_table_id)

        print("Bulk update complete")
        return True
    
    def merge_with_final(self, temp_table_id, final_table_id):
        print(f"Merging updates from {temp_table_id} into {final_table_id}")

        merge_query = f"""
        MERGE `{final_table_id}` T
        USING `{temp_table_id}` S
        ON T.res_id = S.res_id
        WHEN MATCHED THEN
        UPDATE SET
            T.res_bio = S.res_bio,
            T.res_bio_search = LOWER(S.res_bio)
        """

        query_job = self.client.query(merge_query)
        query_job.result()
        print(f"Merged updates into {final_table_id}")

    def create_temp_table(self, temp_table_id):
        schema = [
            bigquery.SchemaField("res_id", "STRING"),
            bigquery.SchemaField("res_bio", "STRING"),
        ]
        
        table = bigquery.Table(temp_table_id, schema=schema)
        table = self.client.create_table(table, exists_ok=True)

        print(f"Temporary table {temp_table_id} created")

    def load_updates_into_temp_table(self, temp_table_id, updates):

        updates = {update["res_id"]: update for update in updates}.values()
        rows_to_insert = [
            {
                "res_id": update["res_id"],
                "res_bio": update["res_bio"],
            }
            for update in updates
        ]

        retries = 5
        retries_left = retries
        for i in range(retries):
            try:
                errors = self.client.insert_rows_json(temp_table_id, rows_to_insert)
                if errors:
                    raise RuntimeError(f"Error inserting rows: {errors}")
                print(f"Inserted {len(rows_to_insert)} rows into {temp_table_id}")
                return
            except Exception as e:
                retries_left -= 1
                print(f"Error inserting rows: {e}")
                print(f"Retrying... ({i + 1}/{retries})")
                time.sleep(10)
            
        raise RuntimeError(f"Failed to insert rows after {retries} retries")

    def merge_updates(self, target_table_id, temp_table_id):
        print(f"Merging updates from {temp_table_id} into {target_table_id}")

        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{temp_table_id}` S
        ON T.res_id = S.res_id
        WHEN MATCHED THEN
        UPDATE SET
            T.res_bio = S.res_bio
        WHEN NOT MATCHED THEN
        INSERT (res_id, res_bio)
        VALUES (S.res_id, S.res_bio)
        """

        query_job = self.client.query(merge_query)
        query_job.result()
        print(f"Merged updates into {target_table_id}")

    def drop_temp_table(self, temp_table_id):
        self.client.delete_table(temp_table_id, not_found_ok=True)
        print(f"Temporary table {temp_table_id} deleted")