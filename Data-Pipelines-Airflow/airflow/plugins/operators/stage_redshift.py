from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = '',
                 aws_credentials_id: str = '',
                 table: str = '',
                 s3_bucket: str = '',
                 s3_key: str = '',
                 region: str = '',
                 json_option: str = 'auto',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from destination Redshift table: {self.table}")
        redshift_hook.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copying data from S3 to Redshift table: {self.table}")
        rendered_key = self.s3_key.format(**context)
        if self.s3_bucket and rendered_key:
            s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        else:
            s3_path = self.s3_bucket  # Falls back to the path if bucket/key aren't provided

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_option
        )
        
        redshift_hook.run(formatted_sql)
        self.log.info(f"Copy to Redshift table {self.table} complete.")
