import requests
import time
import logging
import re

from pathlib import Path
from google.cloud import storage
from google.cloud import bigquery


class LeanplumExporter(object):

    FINISHED_STATE = "FINISHED"
    DEFAULT_SLEEP_SECONDS = 10
    DEFAULT_EXPORT_FORMAT = "csv"
    FILENAME_RE = (r"^https://leanplum_export.storage.googleapis.com"
                   "/export-.*-output([a-z0-9]+)-([0-9]+)$")

    def __init__(self, app_id, client_key):
        self.app_id = app_id
        self.client_key = client_key
        self.filename_re = re.compile(LeanplumExporter.FILENAME_RE)

    def export(self, date, bucket, prefix, dataset, table_prefix,
               version, export_format=DEFAULT_EXPORT_FORMAT):
        job_id = self.init_export(date, export_format)
        file_uris = self.get_files(job_id)
        tables = self.save_files(file_uris, bucket, prefix, date, export_format, version)
        self.create_external_tables(bucket, prefix, date, tables, dataset, table_prefix, version)

    def init_export(self, date, export_format):
        export_init_url = (f"http://www.leanplum.com/api"
                           f"?appId={self.app_id}"
                           f"&clientKey={self.client_key}"
                           f"&apiVersion=1.0.6"
                           f"&action=exportData"
                           f"&startDate={date}"
                           f"&exportFormat={export_format}")

        logging.info("Export Init URL: " + export_init_url)
        response = requests.get(export_init_url)
        response.raise_for_status()

        # Will hard fail if not present
        return response.json()["response"][0]["jobId"]

    def get_files(self, job_id, sleep_time=DEFAULT_SLEEP_SECONDS):
        export_retrieve_url = (f"http://www.leanplum.com/api?"
                               f"appId={self.app_id}"
                               f"&clientKey={self.client_key}"
                               f"&apiVersion=1.0.6"
                               f"&action=getExportResults"
                               f"&jobId={job_id}")

        logging.info("Export URL: " + export_retrieve_url)
        loading = True

        while(loading):
            response = requests.get(export_retrieve_url)
            response.raise_for_status()

            state = response.json()['response'][0]['state']
            if (state == LeanplumExporter.FINISHED_STATE):
                logging.info("Export Ready")
                loading = False
            else:
                logging.info("Waiting for export to finish...")
                time.sleep(sleep_time)

        return response.json()['response'][0]['files']

    def save_files(self, file_uris, bucket_name, prefix, date, export_format, version):
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        datatypes = set()

        version_str = f"v{version}"
        if prefix:
            prefix = self.add_slash_if_not_present(prefix) + version_str
        else:
            prefix = version_str

        prefix += f"/{date}"
        self.delete_gcs_prefix(client, bucket, prefix)

        for uri in file_uris:
            logging.info(f"Retrieving URI {uri}")

            parsed = self.filename_re.fullmatch(uri)
            if parsed is None:
                raise Exception((f"Expected uri matching {LeanplumExporter.FILENAME_RE}"
                                 f", but got {uri}"))

            datatype, index = parsed.group(1), parsed.group(2)
            local_filename = f"{datatype}/{index}.{export_format}"
            datatypes |= set([datatype])

            f = Path(local_filename)
            base_dir = Path(datatype)
            base_dir.mkdir(parents=True, exist_ok=True)

            with requests.get(uri, stream=True) as r:
                r.raise_for_status()
                with f.open("wb") as opened:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            opened.write(chunk)

            logging.info(f"Uploading to gs://{bucket_name}/{prefix}/{local_filename}")
            blob = bucket.blob(f"{prefix}/{local_filename}")
            blob.upload_from_filename(local_filename)

            f.unlink()
            base_dir.rmdir()

        return datatypes

    def delete_gcs_prefix(self, client, bucket, prefix):
        max_results = 1000
        blobs = list(client.list_blobs(bucket, prefix=prefix, max_results=max_results))

        if len(blobs) == max_results:
            raise Exception((f"Max result of {max_results} found at gs://{bucket.name}"
                             f"/{prefix}, increase limit or paginate"))

        bucket.delete_blobs(blobs)

    def create_external_tables(self, bucket_name, prefix, date, tables,
                               dataset, table_prefix, version):
        if table_prefix:
            table_prefix += "_"
        else:
            table_prefix = ""

        gcs_loc = f"gs://{bucket_name}/{prefix}/v{version}/{date}"

        client = bigquery.Client()

        dataset_ref = client.dataset(dataset)

        for leanplum_name in tables:
            table_name = f"{table_prefix}{leanplum_name}_v{version}_{date}"
            logging.info(f"Creating table {table_name}")

            table_ref = bigquery.TableReference(dataset_ref, table_name)
            table = bigquery.Table(table_ref)

            client.delete_table(table, not_found_ok=True)

            external_config = bigquery.ExternalConfig('CSV')
            external_config.source_uris = [f"{gcs_loc}/{leanplum_name}/*"]
            external_config.autodetect = True

            table.external_data_configuration = external_config

            client.create_table(table)

    def add_slash_if_not_present(self, val):
        if not val.endswith("/"):
            val = f"{val}/"
        return val
