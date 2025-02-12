"""
Global Slack notification utility for Airflow DAGs
"""

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# from airflow.models import Variable
import logging
import time

logger = logging.getLogger(__name__)

# The actual URL of the Airflow instance
AIRFLOW_BASE_URL = "https://airflow.yourdomain.com"

# SLACK_WEBHOOK = Variable.get('slack_webhook_url')

def send_slack_alert(context):
    """
    Send Slack alert for failed Airflow tasks with verification delay
    """
    logger.info("Entering slack_alert function")
    try:
        # Add delay to allow task state to stabilize
        time.sleep(5)  
        
        task_instance = context['task_instance']
        
        # Verify task is actually in failed state
        if task_instance.state != "failed":
            logger.info(f"Task {task_instance.task_id} is not in failed state (current state: {task_instance.state}), skipping notification")
            return
            
        dag_id = context['dag'].dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date']

        # Get the original log URL and replace localhost with the actual URL of the Airflow instance
        original_log_url = task_instance.log_url
        log_url = original_log_url.replace("http://localhost:8080", AIRFLOW_BASE_URL)

        slack_msg = f"""
:red_circle: *Task Failed* 
*DAG*: {dag_id}
*Task*: {task_id}
*Execution Date*: {execution_date}
*Error*: The task failed. Check the logs for more details.
*Logs*: {log_url}
        """

        logger.info("Sending Slack notification using SlackWebhookOperator")
        slack_alert = SlackWebhookOperator(
            task_id='slack_alert',
            slack_webhook_conn_id='slack_webhook',
            message=slack_msg,
        )

        # Execute the operator
        slack_alert.execute(context=context)
        logger.info("Slack notification sent successfully")

    except Exception as e:
        logger.error(f"Error in slack_alert function: {str(e)}", exc_info=True)
        raise
