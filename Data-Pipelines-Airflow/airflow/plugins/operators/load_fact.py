from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 table: str = "",
                 sql_load_query: str = "",
                 truncate_before_load: bool = True,
                 *args, **kwargs):
        """
        Initialize the LoadFactOperator.

        :param redshift_conn_id: Connection ID for Redshift.
        :param table: Target table name for loading data.
        :param sql_load_query: SQL query for loading data into the fact table.
        :param truncate_before_load: Specifies whether to truncate the table before loading data.
        """
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_load_query = sql_load_query
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        """
        Executes the loading of data into a fact table in Redshift.
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_load:
            self.log.info(f"Truncating table {self.table} before loading data")
            truncate_query = f"TRUNCATE TABLE {self.table}"
            redshift_hook.run(truncate_query)
        
        self.log.info(f"Loading data into fact table {self.table}")
        load_query = f"INSERT INTO {self.table} {self.sql_load_query}"
        redshift_hook.run(load_query)

        self.log.info(f"Data loading into fact table {self.table} complete.")
