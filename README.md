# Finance Report Data Processor

This repository implements an Airflow DAG workflow that automates the creation of multiple financial reports by combining Python and Bash scripts. The DAG, defined in [airflow_data_processor.py](airflow_data_processor.py), orchestrates different ETL processes using Python scripts located in the **python** subfolder: [etl_retention_and_sunset.py](python/etl_retention_and_sunset.py) and [etl_stock_flow_reports.py](python/etl_stock_flow_reports.py), as well as a collection of Bash scripts in the **bash_script** subfolder: [transfer.sh](bash_script/transfer.sh), [rename_tmp.sh](bash_script/rename_tmp.sh), and [report_merged_non_bundle.sh](bash_script/report_merged_non_bundle.sh).

Alerts for failed DAG tasks are sent via Slack using the notifier utility defined in [slack_notifier.py](utilities/slack_notifier.py).

The project is structured to ensure seamless execution of ETL tasks using Airflow as the orchestrator. The final tables processed by the DAG can be visualized using BI tools such as Tableau or Power BI.