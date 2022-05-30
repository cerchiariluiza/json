import sys
import datetime
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    StringType,
    IntegerType,
)
from pyspark.sql.functions import col, lit


# params job
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENVIRONMENT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
environment = args["ENVIRONMENT"]
job.init(args["JOB_NAME"], args)

#config datas
#toDO
dt_yesterday_referencia = datetime.datetime.now() - datetime.timedelta(days=1)
year_yesterday_referencia = dt_yesterday_referencia.year
month_yesterday_referencia = str(dt_yesterday_referencia.month).zfill(2)
day_yesterday_referencia= str(dt_yesterday_referencia.day).zfill(2)
date_yesterday_referencia = "%s%s%s" % (year_yesterday_referencia,  month_yesterday_referencia, day_yesterday_referencia)
date_yesterday_referencia_query = "%s%s%s%s%s%s" % (year_yesterday_referencia,'-',month_yesterday_referencia, '-',day_yesterday_referencia, '%')

#normalizacao de json para parquet
#input_df= "%s%s%s%s"%('s3://dadosrelacionamento-',environment,\
                      '/topics/emprestimos-e-financiamentos-data-transfer-contrato-financiamento-open-banking/anomesdia=',date_yesterday_referencia)
#input json
#fonte_em_json = glueContext.create_dynamic_frame_from_options(\
                            #connection_type = "s3", 
                            #connection_options = {"paths": [input_df]}, format = "json")
#order = fonte_em_json.toDF()
#order.show()


#boto3 acesso a bucket
#normalizacao de json para parquet
#environment = 'des'
bucket_df= "%s%s"%('dadosrelacionamento-',environment)
path  "%s%s"%('/topics/emprestimos-e-financiamentos-data-transfer-contrato-financiamento-open-banking/anomesdia=',date_yesterday_referencia)
s3 = boto3.resource('s3')
s3_bucket_name=bucket_df
my_bucket=s3.Bucket(s3_bucket_name)
bucket_list = []

#iterates through the files in the bucket
list_remove_linhas = []
for obj in my_bucket.objects.filter(Delimiter='/', Prefix=path):
    key = obj.key
    body = obj.get()['Body'].read()
    for line in obj.get()['Body']._raw_stream:
        data = literal_eval(line.decode('utf8'))
        result = json.dumps(data)
        list_remove_linhas.append(result)
# # print(prefix_df)
df = spark.read.json(sc.parallelize(list_remove_linhas), multiLine=True)
df.count()

from pyspark.sql.functions import * 
from pyspark.sql.functions import lpad
df = df.withColumn('year', lit(year_yesterday_referencia))\
            .withColumn('month', lpad(lit(month_yesterday_referencia),2, '0'))\
            .withColumn('day', lpad(lit(day_yesterday_referencia),2, '0'))

#saida
dif = DynamicFrame.fromDF(df, glueContext, "test_nest")
output_df= "%s%s%s"%('s3://pessoafisicabucket-',environment,\
                       '/plano/financiamento/conversor/')
saida_em_parquet= glueContext.write_dynamic_frame.from_options(frame = dif, 
                            connection_type = "s3", 
                            connection_options = \
                            {"path": output_df,
                            "compression": "snappy",
                            "partitionKeys": ["year", "month", "day"]},
                            format="glueparquet",
transformation_ctx = "saida_em_parquet")

