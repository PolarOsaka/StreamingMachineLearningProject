# Dengan DAG sederhana ini, 
# kita memastikan bahwa mekanisme Airflow (scheduler, parser, executor, UI) semuanya bekerja dengan baik.
# Sehingga kita bisa mengisolasi error berikutnya sebagai error ETL

from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="hello_airflow", # Nama DAG yang muncul di UI
    start_date=datetime(2026, 7, 15), # DAG valid mulai tanggal ini
    schedule=None, # Tidak ada jadwal otomatis, DAG dijalankan secara manual
    catchup=False, # Default, untuk mengantisipasi jika DAG dijalankan di 16 Juli atau seterusnya. Sehingga airflow tidak perlu mengejar atau melakukan proses dari hari2 yg sudah lewat hingga 16 Juli
    tags=["tutorial"],
)
def hello_airflow():
    @task
    def start():
        print("Starting DAG...")
    @task
    def hello():
        print("Hello Airflow!")
    @task
    def finish():
        print("Finished DAG!")

    # Instantiate task
    start_task = start()

    hello_task = hello()

    finish_task = finish()

    # Dependency
    start_task >> hello_task >> finish_task

hello_airflow()

# Untuk jalankan di command, run: docker compose exec airflow-scheduler airflow dags trigger hello_airflow