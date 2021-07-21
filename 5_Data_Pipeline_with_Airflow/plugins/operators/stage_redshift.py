from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Operator to get data from AWS S3 and stage it into Redshift. 

    Args:
        redshift_conn_id (str): Postgres connection name created by the user on Airflow; 
        aws_conn_id (str): AWS connection name created by the user on Airflow;
        table (str): table in Redshift where the data should be staged into;
        s3_bucket (str): s3 bucket where the source data resides;
        s3_key (str): s3 key to identify the desired data inside the bucket;
        ignore_header (int): how many rows should be considered as headers;
        delimiter (str): delimeter type Redshift should use;
        json_type (str): method for reading the JSON file(s). 
            Ether AUTO or JSON file path which maps files and paths; 
    """

    template_fields = ('s3_key')
    copy_template = """
        COPY {},
        FROM '{}'
        ACCESS_KEY_ID = '{}'
        SECRET_ACCESS_KEY = '{}'
        IGNOREHEADER {}
        DELIMETER '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
            redshift_conn_id = "",
            aws_conn_id = "",
            table = "",
            s3_bucket = "",
            s3_key = "",
            ignore_header = 1,
            delimiter = ",",
            json_type = "auto",
            *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_header = ignore_header
        self.delimeter = delimiter
        self.json_type = json_type

    def execute(self, context):  
        self.log.info('StageToRedshiftOperator starting')
        
        # stablish the connections with AWS, Redshift and get the credentials for execute the COPY statements
        self.log.info(f'StageToRedshiftOperator connecting with {self.aws_conn_id} and {self.redshift_conn_id}')
        aws_hook = AwsHook(aws_conn_id = self.aws_conn_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info(f'Clearing data from destination table {self.table}')
        redshift_hook.run(f'DELETE FROM {self.table}')

        self.log.info('Copying data from S3 to Redsift')
        s3_key_rendered = self.s3_key.format(**context)
        path = f's3://{self.s3_bucket}/{s3_key_rendered}'
        formatted_query = StageToRedshiftOperator.copy_template.format(
            self.table,
            path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.ignore_header,
            self.delimeter
        )
        redshift_hook.run(formatted_query)



