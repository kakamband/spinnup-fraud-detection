import airflow

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import BigQueryRowSensor
from airflow.models import Variable
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import file_to_gcs
from airflow.contrib.operators import gcs_download_operator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import bigquery_table_delete_operator

DIR = '{{ var.value.data_dir }}/spinnup_fraud_detection{{ds_nodash}}' 

START_DATE = datetime(2020, 1, 1, 0, 0, 0)
ds = str('{{ ds }}') 

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'schedule_interval': None,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id = 'spinnup_fraud_detection_v01',
          description = 'Detect fraudulent activity and identify type of fraud',
          schedule_interval = '0 15 * * *',
          max_active_runs = 1,
          default_args = DEFAULT_ARGS
)

# Sensor to check for data in BQ
spinnup_partition_sensor = BigQueryRowSensor(
    task_id='spinnup_partition_sensor',
    project_id='umg-spinnup',
    dataset_id='spotify',
    table_id='spotify_trends${{ ds_nodash }}',
    row_count=100000,
    poke_interval=600,
    bigquery_conn_id='bigquery_default',
    pool='sensors',
    dag=dag
)

# Generate dataset
generate_dataset = bigquery_operator.BigQueryOperator(
    task_id='generate_dataset',
    sql='sql/spinnup/fraud_detection_v01_data.sql',
    destination_dataset_table='umg-data-science.detect_fraud_spinnup.f1_tracks${{ ds_nodash }}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bigquery_conn_id='bigquery_default',
    use_legacy_sql=False,
    dag=dag
)

# Make a temp directory
mkdir = BashOperator(
    task_id='mkdir',
    bash_command='mkdir -p {}'.format(DIR),
    dag=dag
)

# Download python script from GCS
download_script = gcs_download_operator.GoogleCloudStorageDownloadOperator(
    task_id='download_python_script',
    bucket='{{ var.value.project_bucket }}',
    object='builds/spinnup/fraud_detection/v01.py',
    filename='{}/v01.py'.format(DIR),
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag
)

# Run python script
generate_results = BashOperator(
	task_id ='generate_results',
	bash_command = 'cd {} && python v01.py'.format(DIR),
    env={'DIR': DIR, 'ds': ds},
	dag = dag
)

# Upload results to GCS
upload_to_gcs = file_to_gcs.FileToGoogleCloudStorageOperator(
    task_id = 'upload_to_gcs',
    dst = 'data/spinnup/fraud_detection_v01/year={{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y\') }}/month={{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y%m\') }}/day={{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y%m%d\') }}/v01_results{{ds_nodash}}.csv',
    bucket = '{{ var.value.project_bucket }}',
    conn_id = 'google_cloud_default',
    src = '%s/v01_results.csv' % (DIR),
    dag = dag
)

# Load results from GCS to BQ
load_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_bq',
    bucket='{{ var.value.project_bucket }}',
    source_objects=['data/spinnup/fraud_detection_v01/year={{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y\') }}/month={{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y%m\') }}/day={{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y%m%d\') }}/v01_results{{ds_nodash}}.csv'],
    destination_project_dataset_table='umg-data-science.detect_fraud_spinnup.v01_results${{ ds_nodash }}', 
    schema_object='builds/flow/schemas/spinnup/spinnup_fraud_detection_v01_schema.json',
    source_format='CSV',
    field_delimiter = ',',
    bigquery_conn_id = 'bigquery_default',
    skip_leading_rows=0,
    write_disposition='WRITE_TRUNCATE', 
    dag=dag
)

# Delete directory from airflow instance
delete_directory = BashOperator(
    task_id = 'delete_directory_from_instance',
    bash_command = 'rm -rf {}'.format(DIR),
    dag = dag
)

# dag structure
spinnup_partition_sensor >> generate_dataset >> mkdir >> download_script >> generate_results >> upload_to_gcs >> load_to_bq >> delete_directory