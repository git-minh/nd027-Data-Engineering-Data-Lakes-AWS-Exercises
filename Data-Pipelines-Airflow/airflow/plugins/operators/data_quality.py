from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 tests: list = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        
    def execute(self, context):
        if not self.tests:
            raise ValueError("No data quality tests provided")
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        failing_tests = []
        for test in self.tests:
            sql = test.get("check_sql")
            exp_result = test.get("expected_result")
            
            records = redshift_hook.get_records(sql)
            
            if not records or not len(records):
                raise ValueError(f"Data quality check failed: No results returned for SQL: {sql}")

            actual_result = records[0][0]
            if actual_result != exp_result:
                failing_tests.append(sql)
                self.log.error(f"Data quality check failed for SQL: {sql}, Expected: {exp_result}, Got: {actual_result}")
            else:
                self.log.info(f"Data quality check passed for SQL: {sql}, Expected: {exp_result}, Got: {actual_result}")
        
        if failing_tests:
            raise ValueError(f"Data quality check failed for {len(failing_tests)} out of {len(self.tests)} tests.")
        
        self.log.info("Data quality checks completed successfully.")
