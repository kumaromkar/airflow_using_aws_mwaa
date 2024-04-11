import json
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def task_failure_slack_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

    environment = Variable.get("airflow_environment", default_var="local")
    if environment not in ["dev", "prod"] :
        print(f"{environment} mode - skip sending alerts.")
        return

    ti = context['ti'] # to get the Task Instance
    TASK_STATE = ti.state
    TASK_ID = context.get('task_instance').task_id
    DAG_ID = context.get('task_instance').dag_id
    EXECUTION_TIME = context.get('execution_date')
    LOG_URL = context.get('task_instance').log_url

    slack_msg = f"""
        :red_circle: Task Failed.
        *Dag*: {DAG_ID}
        *Task*: {TASK_ID}
        *Task State*: {TASK_STATE}
        *Execution Time*: {EXECUTION_TIME}
        *Log URL*: <{LOG_URL}|*Logs*>
    """

    SLACK_FAILURE_ALERT_CONN_ID = "slack_dms_monitor"
    CHANNEL = BaseHook.get_connection(SLACK_FAILURE_ALERT_CONN_ID).login

    slack_alert = SlackWebhookOperator(
        task_id=TASK_ID,
        slack_webhook_conn_id = SLACK_FAILURE_ALERT_CONN_ID,
        message=slack_msg,
        channel=CHANNEL,
        username='airflow'
    )

    return slack_alert.execute(context=context)

default_args = {
    'owner':'data platform',
    'retries': 0,
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
}

@dag(
    dag_id="example_taskflow_api",
    description="Taskflow api example",
    schedule=None,
    default_args = default_args,
    catchup=False,
    on_success_callback=None,
    on_failure_callback=task_failure_slack_alert,
    tags=["example"],
)

def tutorial_taskflow_api():
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

tutorial_taskflow_api()
