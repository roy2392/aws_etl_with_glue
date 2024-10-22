import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import psycopg2
import pandas as pd

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

host = 'glue-mlops-2.cluster-csnhj38wg3oc.us-east-1.rds.amazonaws.com'
port = 5432
database = 'MLOps_db'
user = 'postgres'
password = 'postgres'

with psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, database, user, password)) as conn:
    query = "SELECT * FROM Users"
    df = pd.read_sql_query(query, conn)

job.commit()