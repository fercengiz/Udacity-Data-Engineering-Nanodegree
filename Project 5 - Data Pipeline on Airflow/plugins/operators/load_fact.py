from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """Custom Operator for loading data into fact tables.     
    Attributes:
        ui_color (str): color code for task in Airflow UI.
        insert_template (str): template string for inserting data.     
    """
    ui_color = '#F98866'
    insert_template = "INSERT INTO {} ({});"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table = '',
                 query = '',
                 truncate=True,
                 *args, **kwargs):
        """Initializes a Fact Load Operator.
        Args:
            redshift_conn_id (str): Airflow connection ID for redshift database.
            table (str): Name of fact table.
            query (str): Query used to populate fact table.
            truncate (bool): Flag that determines if table is truncated before insert.
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        """Executes task for loading a fact table.
        Args:
            context (:obj:`dict`): Dict with values to apply on content. 
        """
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info('Truncating table {}'.format(self.table))
            redshift.run('TRUNCATE {}'.format(self.table))
        
        insert_query = LoadFactOperator.insert_template.format(self.table, self.query)
        self.log.info('Loading Fact table: {} with {}'.format(self.table,self.query))
        redshift.run(insert_query)
        self.log.info('Successfully Loaded Fact table: {}'.format(self.table))
