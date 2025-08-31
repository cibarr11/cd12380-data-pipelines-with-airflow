from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 table=None,
                 insert_sql=None,
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = (insert_sql or "").rstrip(" ;")
        self.append_only = append_only

    def execute(self, context):
        if not self.table or not self.insert_sql:
            raise ValueError("parameter error")
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            hook.run(f"TRUNCATE TABLE {self.table};")
        sql = f"INSERT INTO {self.table} {self.insert_sql}"
        hook.run(sql)
        
