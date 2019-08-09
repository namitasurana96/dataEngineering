from google.cloud import bigquery


def bq_create_dataset():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_dataset_id')

    try:
        bigquery_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
#bq_create_dataset()

def bq_create_table():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_dataset_id')

    # Prepares a reference to the table
    table_ref = dataset_ref.table('StockPrice')

    try:
        bigquery_client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('pred', 'FLOAT', mode='REQUIRED')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))

bq_create_table()

BQ_TABLE_SCHEMA = [
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED', description='Values'),
    bigquery.SchemaField('pred', 'FLOAT', mode='REQUIRED', description='Values'),
    bigquery.SchemaField('created_at', 'TIMESTAMP', mode='REQUIRED', description='Date and time when the record was created')
]

def export_items_to_bigquery():
    # Instantiates a client
    bigquery_client = bigquery.Client()

    # Prepares a reference to the dataset
    dataset_ref = bigquery_client.dataset('my_dataset_id')

    table_ref = dataset_ref.table('my_table_name')
    table = bigquery_client.get_table(table_ref)  # API call

    rows_to_insert = [
        ('Phred Phlyntstone', 32),
        (u'Wylma Phlyntstone', 29),
    ]
    errors = bigquery_client.insert_rows(table, rows_to_insert)  # API request
    assert errors == []

#export_items_to_bigquery()