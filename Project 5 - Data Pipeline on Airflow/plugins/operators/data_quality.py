from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Custom Operator for performing data quality check on a table.
    Attributes:
        ui_color (str): color code for task in Airflow UI.
        count_template (str): template string for checking if table contains data.
        null_count_template (str): template string for counting nulls on given column.
    """
    ui_color = '#89DA59'
    count_template = " SELECT COUNT(*) FROM {}"
    null_count_template = " SELECT COUNT(*) FROM {} WHERE {} is null "
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 dq_checks = [],
                 *args, **kwargs):
        """Initializes a Data Quality Check Operator.
        Args:
            redshift_conn_id (str): Airflow connection ID for redshift database.
            dq_checks (list): Name of table, column name and expected result for quality check.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        """Executes task for data quality check.
        Args:
            context (:obj:`dict`): Dict with values to apply on content.        
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for check in self.dq_checks:
            table = check['table']
            column = check['column']
            exp = check["expected_result"]
            
            self.log.info('Fetching record count for table {}'
                          .format(table))
            records = redshift.get_records(
                DataQualityOperator.count_template.format(table))
            num_records = records[0][0]
            
            
            self.log.info('Checking if table {} has record count > 0'
                          .format(table))
            if num_records <1:
                raise ValueError('Table load failed: 0 rows in {} table'
                                 .format(table))
            else: 
                self.log.info('Record count for table {} is {}'
                              .format(table,num_records))
                self.log.info('Fetching null record count for table {} for {}'
                              .format(table,column))
                null_records = redshift.get_records(
                    DataQualityOperator.null_count_template.format(table,column))
                num_null_records = null_records[0][0]

                if num_null_records != exp:
                    raise ValueError(
                        '''Data quality check failed. Null count for column {} 
                        on table {} Expected : {} | Got: {}'''
                        .format(column, table, exp, num_null_records))
                else:
                    self.log.info(
                        '''Null data quality for column {} 
                        on table {} check passed with {} records'''
                        .format(column, table, exp, num_null_records))