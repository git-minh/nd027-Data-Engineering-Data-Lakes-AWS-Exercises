from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 table: str = '',
                 sql_load_query: str = '',
                 truncate_before_load: bool = True,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_load_query = sql_load_query
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_load:
            truncate_query = f"TRUNCATE TABLE {self.table}"
            self.log.info(f"Truncating Redshift table: {self.table}")
            redshift_hook.run(truncate_query)

        load_query = f"INSERT INTO {self.table} {self.sql_load_query}"
        self.log.info(f"Loading data into {self.table} dimension table on Redshift")
        redshift_hook.run(load_query)

        self.log.info(f"LoadDimensionOperator complete: {self.table} has been loaded.")
