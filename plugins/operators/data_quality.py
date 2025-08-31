from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

# Operators for use in the final py script
OPS = {
    '>': operator.gt,
    '>=': operator.ge,
    '=': operator.eq,
    '==': operator.eq,
    '!=': operator.ne,
    '<': operator.lt,
    '<=': operator.le,
}

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tests=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        if not self.tests:
            self.log.warning("No data quality tests provided; skipping.")
            return

        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #store failures for returning back to user
        failures = []

        for idx, test in enumerate(self.tests, start=1):
            #checking number of rows
            sql = test.get("check_sql")
            op_symbol = test.get("op", ">")
            expected = test.get("expected", 0)
            desc = test.get("desc", f"Test #{idx}")

            self.log.info("data quality check: %s", desc)
            self.log.debug("SQL: %s | Expect: result %s %s", sql, op_symbol, expected)

            result = hook.get_first(sql)

            if not result or len(result) == 0:
                failures.append(f"{desc}: no rows")
                continue

            actual = result[0]
            if not OPS[op_symbol](actual, expected):
                failures.append(f"{desc}: got {actual}, anticipated {op_symbol} {expected}")

        if failures:
            error_message = "Data quality checks failed:\n- " + "\n- ".join(failures)
            self.log.error(error_message)
            raise ValueError(error_message)

        self.log.info("All data quality checks passed successfully.")
