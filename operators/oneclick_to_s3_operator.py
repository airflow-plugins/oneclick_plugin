import json

from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.models import Connection, BaseOperator

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook


def getParams(returnable):
    oc_conn = get_conn('astronomer-oneclick')
    if returnable == 'client_uuid':
        return oc_conn.extra_dejson.get('client_uuid')
    elif returnable == 'client_api_key':
        return oc_conn.extra_dejson.get('client_api_key')


@provide_session
def get_conn(conn_id, session=None):
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first())
    return conn


class OneClickToS3Operator(BaseOperator):
    """
    One Click Retail to S3 Operator
    :param one_click_conn_id:   The id for the One Click connection. The
                                relevant credentials should be put in the
                                extra section of the connection.
                                (e.g. {"client_uuid":"X","client_api_key":"X"})
    :type one_click_conn_id:    string
    :param start_date:          The requested end date to return data for
                                (Format: yyyy-mm-dd eg. 2017-04-10).
                                This cannot be used with the weeks_back parameter.
    :type start_date:           string
    :param end_date:            The requested end date to return data for
                                (Format: yyyy-mm-dd eg. 2017-04-10).
                                This cannot be used with the weeks_back parameter.
    :type end_date:             string
    :param weeks_back:          The number of weeks back from which to pull
                                data. For example, if set to "6", this will pull
                                all records from 6 weeks from the date on which
                                the DAG runs (NOT the scheduled exectution date).
                                This cannot be used with the start/end_date parameters.
    :type weeks_back:           string
    :param filter_id:           (Optional) The relevant OneClick filter id.
    :type filter_id:            string
    """

    template_fields = ['one_click_payload',
                       's3_key']

    @apply_defaults
    def __init__(self,
                 one_click_conn_id,
                 one_click_endpoint,
                 one_click_payload={},
                 s3_conn_id=None,
                 s3_bucket=None,
                 s3_key=None,
                 **kwargs):
        super(OneClickToS3Operator, self).__init__(**kwargs)
        self.one_click_conn_id = one_click_conn_id
        self.one_click_endpoint = one_click_endpoint
        self.one_click_payload = one_click_payload
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        conn = HttpHook(method='GET', http_conn_id=self.one_click_conn_id)
        headers = {}
        headers['X-API-KEY'] = getParams('client_api_key')
        uuid = getParams('client_uuid')
        payload = self.one_click_payload

        if self.one_click_endpoint == 'reports_export':
            endpoint = ''.join(['v5/clients',
                                uuid,
                                '/reports/export'])
            payload['format'] = 'csv'
            r = conn.run(endpoint, data=payload)
            rows = r.text.strip().split('\n\n')
            rows = [i for i in rows if i]

            if len(rows) > 2:
                raise Exception('One dataset accepted. {0} provided.'
                                .format(len(rows)))

            with open('temp.csv', 'w') as f:
                f.write(rows[1])
                output_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
                output_s3.load_file(
                    filename='temp.csv',
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True
                )
                output_s3.connection.close()
        else:
            if self.one_click_endpoint == 'sales_and_share_rollup':
                endpoint_name = 'rollup'
            elif self.one_click_endpoint == 'sales_and_share_period':
                endpoint_name = 'period'

            endpoint = ''.join(['v4/clients/',
                                uuid,
                                '/reports/sales_and_share_{0}'.format(endpoint_name)])
            r = conn.run(endpoint, data=payload, headers=headers)
            s3 = S3Hook(s3_conn_id=self.s3_conn_id)
            output = (json.dumps(json.loads(r.text)['data']))
            s3.load_string(
                string_data=output,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
            s3.connection.close()
