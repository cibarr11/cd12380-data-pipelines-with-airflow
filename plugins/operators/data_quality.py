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
                  redshift_conn_id="redshift", tables=[],*args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        # Added redshift connection id for reference
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        # Example logic (to be replaced with actual data quality checks)
        self.log.info('DataQualityOperator not implemented yet')

        # verify for failures
        for table in self.tables:
            # define empty list to store the failures
            failures = []
            # failure to verify is the table is not empty
            verify_table = hook.get_first(f"SELECT COUNT(*) FROM {table}")

            if verify_table is None or len(verify_table) == 0:
                failures.append(f"{table}: no data")
            elif verify_table[0] is None or verify_table[0] <= 0:
                failures.append(f"{table}: incorrect number of rows")
        
        if failures:
            raise ValueError("Failures found")
        else:
            self.log.info("All data quality checks passed.")