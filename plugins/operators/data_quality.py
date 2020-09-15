from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failed_tests = []
        self.log.info('Running test(s)')
        for test in self.tests:
            test_sql = test['check_sql']
            expected_result = test['expected_result']
            
            query_result = redshift.get_records(test_sql)[0]
            
            if test['check_equality']:
                if expected_result != query_result[0]:
                    failed_tests.append({'test' : test_sql,
                                        'expected_result' : expected_result,
                                        'query_result' : query_result,
                                        'check_equality' : test['check_equality']})
            else:
                if expected_result == query_result[0]:
                    failed_tests.append({'test' : test_sql,
                                        'expected_result' : expected_result,
                                        'query_result' : query_result,
                                        'check_equality' : test['check_equality']})
                
        if len(failed_tests) > 0:
            self.log.error('Test failed')
            for fail in failed_tests:
                self.log.error(f'{fail["test"]} failed. Expected result was {fail["expected_result"]}, query result was {fail["query_result"]}. (Is equality check : {fail["check_equality"]})')