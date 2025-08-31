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
        if not self.tables:
            self.log.warning("No tables provided for data quality checks.")
            return
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failures = []
        for table in self.tables:
            self.log.info("Veriyfing for non-empty rows", table)
            records = hook.get_first(f"SELECT COUNT(*) FROM {table};")
            if records is None or len(records) == 0:
                failures.append(f"{table}:no results")
            elif records[0] is None or records[0] <= 0:
                failures.append(f"{table}: contains 0 rows")

        if failures:
            message = "Data quality check failed"
            raise ValueError(messages)
        self.log.info("All data quality checks passed")