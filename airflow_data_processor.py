"""
Airflow DAG script to automate all the data processing pipeline to create the financial reports
"""

# Libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from utilities.slack_notifier import send_slack_alert

import datetime
import sys
import os


# 'python' folder is where we store the Python scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

# 'bash_script' folder is where we store the Bash scripts
bash_script_path = os.path.join(os.path.dirname(__file__), 'bash_script')

import etl_stock_flow_reports
import etl_retention_and_sunset


# DAG arguments
default_args = {
    'owner': 'Ruddy Gunawan',
    'start_date': datetime.datetime(2025, 1, 1, 12, 10, 0),
    'email': ['ruddy@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'on_failure_callback': send_slack_alert,
}

# DAG definition with interval defined in minutes
dag = DAG(
    'data_processor',
    default_args=default_args,
    description='Data Processor to Generate Financial Reports',
    schedule_interval=timedelta(minutes=75),
    catchup=False,
)

# Task 1 - Copy the bash script to /tmp and make them executable - we need to do this because of file permission issues
task1 = BashOperator(
    task_id='copy_and_chmod_script',
    bash_command=f'cp {os.path.join(bash_script_path, "transfer.sh")} /tmp/transfer.sh && '
                 f'cp {os.path.join(bash_script_path, "rename_tmp.sh")} /tmp/rename_tmp.sh && '
                 f'cp {os.path.join(bash_script_path, "report_merged_non_bundle.sh")} /tmp/report_merged_non_bundle.sh && '
                 f'chmod +x /tmp/transfer.sh /tmp/rename_tmp.sh /tmp/report_merged_non_bundle.sh ',
    dag=dag,
)

# Task 2 - Execute Bash script transfer.sh - for the brand ABC
task2 = BashOperator(
    task_id='run_transfer_abc',
    bash_command='/tmp/transfer.sh ABC ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 3 - Execute Bash script transfer.sh - for the brand DEF
task3 = BashOperator(
    task_id='run_transfer_def',
    bash_command='/tmp/transfer.sh DEF ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 4 - Execute Bash script transfer.sh - for the brand GHI
task4 = BashOperator(
    task_id='run_transfer_ghi',
    bash_command='/tmp/transfer.sh GHI ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 5 - Execute Bash script transfer.sh - for the brand JKL
task5 = BashOperator(
    task_id='run_transfer_jkl',
    bash_command='/tmp/transfer.sh JKL ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 6 - Execute Bash script transfer.sh - for the brand MNO
task6 = BashOperator(
    task_id='run_transfer_mno',
    bash_command='/tmp/transfer.sh MNO ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 7 - Execute Bash script rename_tmp.sh - for the brand ABC
task7 = BashOperator(
    task_id='run_rename_tmp_abc',
    bash_command='/tmp/rename_tmp.sh ABC ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 8 - Execute Bash script rename_tmp.sh - for the brand DEF
task8 = BashOperator(
    task_id='run_rename_tmp_def',
    bash_command='/tmp/rename_tmp.sh DEF ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 9 - Execute Bash script rename_tmp.sh - for the brand GHI
task9 = BashOperator(
    task_id='run_rename_tmp_ghi',
    bash_command='/tmp/rename_tmp.sh GHI ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 10 - Execute Bash script rename_tmp.sh - for the brand JKL
task10 = BashOperator(
    task_id='run_rename_tmp_jkl',
    bash_command='/tmp/rename_tmp.sh JKL ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 11 - Execute Bash script rename_tmp.sh - for the brand MNO
task11 = BashOperator(
    task_id='run_rename_tmp_mno',
    bash_command='/tmp/rename_tmp.sh MNO ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 12 - Execute Python script for Stock Flow Reports - for the brand ABC
task12 = PythonOperator(
    task_id='etl_stock_flow_reports_abc',
    python_callable=etl_stock_flow_reports.run_etl_process_abc,
    dag=dag,
)

# Task 13 - Execute Python script for Stock Flow Reports - for the brand DEF
task13 = PythonOperator(
    task_id='etl_stock_flow_reports_def',
    python_callable=etl_stock_flow_reports.run_etl_process_def,
    dag=dag,
)

# Task 14 - Execute Python script for Stock Flow Reports - for the brand GHI
task14 = PythonOperator(
    task_id='etl_stock_flow_reports_ghi',
    python_callable=etl_stock_flow_reports.run_etl_process_ghi,
    dag=dag,
)

# Task 15 - Execute Python script for Stock Flow Reports - for the brand JKL
task15 = PythonOperator(
    task_id='etl_stock_flow_reports_jkl',
    python_callable=etl_stock_flow_reports.run_etl_process_jkl,
    dag=dag,
)

# Task 16 - Execute Python script for Stock Flow Reports - for the brand MNO
task16 = PythonOperator(
    task_id='etl_stock_flow_reports_mno',
    python_callable=etl_stock_flow_reports.run_etl_process_mno,
    dag=dag,
)

# Task 17 - Execute Bash script report_merged_non_bundle.sh
task17 = BashOperator(
    task_id='run_report_merged_non_bundle',
    bash_command='/tmp/report_merged_non_bundle.sh ',
    dag=dag,
    output_encoding='utf-8',
)

# Task 18 - Execute Python script for Retention and Sunset - for the brand ABC
task18 = PythonOperator(
    task_id='etl_retention_and_sunset_abc',
    python_callable=etl_retention_and_sunset.run_etl_process_abc,
    dag=dag,
)

# Task 19 - Execute Python script for Retention and Sunset - for the brand DEF
task19 = PythonOperator(
    task_id='etl_retention_and_sunset_def',
    python_callable=etl_retention_and_sunset.run_etl_process_def,
    dag=dag,
)

# Task 20 - Execute Python script for Retention and Sunset - for the brand GHI
task20 = PythonOperator(
    task_id='etl_retention_and_sunset_ghi',
    python_callable=etl_retention_and_sunset.run_etl_process_ghi,
    dag=dag,
)

# Task 21 - Execute Python script for Retention and Sunset - for the brand JKL
task21 = PythonOperator(
    task_id='etl_retention_and_sunset_jkl',
    python_callable=etl_retention_and_sunset.run_etl_process_jkl,
    dag=dag,
)

# Task 22 - Execute Python script for Retention and Sunset - for the brand MNO
task22 = PythonOperator(
    task_id='etl_retention_and_sunset_mno',
    python_callable=etl_retention_and_sunset.run_etl_process_mno,
    dag=dag,
)

# Task 23 - Clean up: remove the temporary scripts
task23 = BashOperator(
    task_id='cleanup',
    bash_command='rm /tmp/transfer.sh /tmp/rename_tmp.sh /tmp/report_merged_non_bundle.sh ',
    dag=dag,
)

# Task Pipeline
task1 >> task2 >> task7 >> task12 >> task18
task1 >> task3 >> task8 >> task13 >> task19
task1 >> task4 >> task9 >> task14 >> task20
task1 >> task5 >> task10 >> task15 >> task21
task1 >> task6 >> task11 >> task16 >> task22
[task18, task19, task20, task21, task22] >> task17 >> task23
