from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="redshift",
                 table=None,
                 aws_credentials_id="aws_credentials",
                 s3_bucket=None,
                 s3_key=None,
                 region="us-east-2",
                 json_option="auto",
                 truncate=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        #reference the DAG conn id and log in information
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option
        self.truncate = truncate

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        # check validation for the data
        if not self.table:
            raise ValueError("Table Error")
        if not self.s3_bucket or not self.s3_key:
            raise ValueError("Keys Error")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_conn = BaseHook.get_connection(self.aws_credentials_id)

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        if self.truncate:
            redshift.run(self.table)

        #access the aws credentials
        access_key = aws_conn.login
        secret_key = aws_conn.password

        #pass in copy of log in info
        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            REGION '{self.region}'
            JSON '{self.json_option}';
        """
        #execute the redshift run
        redshift.run(copy_sql)