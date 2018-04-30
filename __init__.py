from airflow.plugins_manager import AirflowPlugin
from OneClickPlugin.operators.oneclick_to_s3_operator import OneClickToS3Operator


class OneClickPlugin(AirflowPlugin):
    name = "oneclick_plugin"
    operators = [OneClickToS3Operator]
