import time
import os
import argparse
from google.cloud import bigquery

# credential file
credential_file = '.config/jf-project-20190218-361593308de1.json'


def tbl_exists(dataset_id, table_id):
    """
    Check if the table already exists
    """
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False


def load_tbl(dataset_id, table_id, local_file):
    table_ref = client.dataset(dataset_id).table(table_id)

    # get the extention of the file
    ext = os.path.splitext(local_file)[1]

    job_config = bigquery.LoadJobConfig()
    if ext.lower() == '.csv':
        job_config.source_format = bigquery.SourceFormat.CSV    
        job_config.skip_leading_rows = 1
    elif ext.lower() == '.parquet':
        job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.autodetect = False

    t0 = time.time()
    with open(local_file, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='australia-southeast1',
            job_config=job_config
        )
    
    job.result()

    print('Loaded {} rows into {}:{}, took {:.2f}s'.format(
        job.output_rows, dataset_id, table_id, time.time() - t0
    ))


if __name__ == '__main__':
    # setup bigquery client
    if os.path.exists(credential_file):
        client = (bigquery
            .Client
            .from_service_account_json(credential_file))
    else:
        # use os env variable
        client = bigquery.Client()

    parser = argparse.ArgumentParser("Load data into bigquery")
    parser.add_argument("-t", "--table", dest="table", 
        default='my_dataset.userdata', help="Fully-qualified destination table name, i.e. dataset.tablename")
    parser.add_argument("-s", "--source", dest="source", 
        default='./data/userdata1.parquet', help="Name of local file to import")
    args = parser.parse_args()

    local_file = args.source
    dataset_id = args.table.split('.')[0]
    table_id = args.table.split('.')[1]

    try:
        if tbl_exists(dataset_id, table_id):
            load_tbl(dataset_id, table_id, local_file)
        else:
            print('{}:{} is not found, please create the table.'.format(
                dataset_id, table_id
            ))
    except Exception as e:
        print('error - ', e)