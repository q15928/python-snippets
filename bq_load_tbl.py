from google.cloud import bigquery
import time

def load_tbl(dataset_id, table_id, local_file):
    client = (bigquery
        .Client
        .from_service_account_json('.config/jf-project-20190218-361593308de1.json'))
    
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
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
    local_file = './data/user.csv'
    dataset_id = 'my_dataset'
    table_id = 'userdata'

    try:
        load_tbl(dataset_id, table_id, local_file)
    except Exception as e:
        print('error - ', e)