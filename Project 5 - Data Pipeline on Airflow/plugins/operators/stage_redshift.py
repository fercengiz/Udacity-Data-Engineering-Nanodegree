from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    """Custom Operator for copying data from S3 buckets to 
    redshift cluster into staging tables.
    Attributes:
        ui_color (str): color code for task in Airflow UI.
        template_fields (:obj:`tuple` of :obj: `str`): list of template parameters.
        copy_sql (str): template string for coping data from S3.
    """
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key="",
                 region='us-west-2',
                 json='auto',
                 ignore_headers=1,
                 table='',
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json = json
        self.ignore_headers = ignore_headers
        self.table = table        
        
    def execute(self, context):
        """Executes task for staging to redshift.
        Args:
            context (:obj:`dict`): Dict with values to apply on content. 
        """
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Truncating table {}'.format(self.table))
        redshift.run('TRUNCATE {}'.format(self.table))
        
        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)
        if self.json != 'auto':
            json = 's3://{}/{}'.format(self.s3_bucket, self.json)
        else:
            json = self.json
            
        self.log.info('Coping data from {} to {} on table Redshift'.format(s3_path, self.table))
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            json
        )
        redshift.run(formatted_sql)
        self.log.info('Successfully Copied data from {} to {} table on Redshift'.format(s3_path, self.table))



