from google.cloud import bigquery

def create_tbl(dataset_id, table_id):
    client = (bigquery.Client
              .from_service_account_json('.config/jf-project-20190218-361593308de1.json'))
    dataset_ref = client.dataset(dataset_id)
 
    schema = [
        bigquery.SchemaField('registration_dttm', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('id', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('first_name', 'STRING', mode='NULLABLE'), 
        bigquery.SchemaField('last_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('email', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('gender', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('ip_address', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('cc', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('birthdate', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('salary', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('comments', 'STRING', mode='NULLABLE')
    ]

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

if __name__ == "__main__":
    dataset_id = 'my_dataset'
    table_id = 'userdata'
    try:
        create_tbl(dataset_id, table_id)
    except Exception as e:
        print("error - ", e)