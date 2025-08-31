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
        # Create a PostgresHook to connect to Redshift
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            # truncating table before running
            hook.run(f"truncate the following: {self.table}")
        # Create the sql statement for refernce into the table
        sql = f"inserting into the table {self.table} {self.insert_sql}"
        # Log the reference that data is being added into the table
        self.log.info("Adding data into the table %s", self.table)
        # Execute
        hook.run(sql)
        
