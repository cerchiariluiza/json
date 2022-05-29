import boto3
import pandas as pd
import io

s3 = boto3.resource('s3')
bucket = s3.Bucket('pessoajuridicabucket-dess')
prefix_objs = bucket.objects.filter(Delimiter='/', Prefix='plano/financiamento/filtrador9/')
prefix_df = []




s3 = boto3.resource('s3')
bucket = s3.Bucket('pessoajuridicabucket-dess')
s3_client =boto3.client('s3')
s3_bucket_name='pessoajuridicabucket-dess'
my_bucket=s3.Bucket(s3_bucket_name)
bucket_list = []
import json

from ast import literal_eval
import json
import json

#iterates through the files in the bucket
my_bucket=s3.Bucket(s3_bucket_name)
prefix_df = []
for obj in my_bucket.objects.filter(Delimiter='/', Prefix='plano/financiamento/filtrador9/'):
    key = obj.key
    body = obj.get()['Body'].read()
#     print(body)
    data = literal_eval(body.decode('utf8'))
    reso = json.dumps(data)
    prefix_df.append(reso)
    
    
# print(prefix_df)
# print(type(prefix_df))
df = spark.read.json(sc.parallelize(prefix_df), multiLine=True)

df.select('agencia_cliente').show()
df.count()



import datetime
import json
from pprint import pprint
from pyspark.sql.functions import lit
import boto3
import pyspark.sql.functions as f 
import sys
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType
from awsglue.dynamicframe import DynamicFrame

df.createOrReplaceTempView("view")
from pyspark.sql import Window


variavel_veiculo = """
SELECT 
cnpj_empresa as numero_cpf,
current_date as data_processamento,
    agencia_cliente as numero_agencia,
    conta_cliente as numero_conta_corrente, 
    digito_conta_cliente as numero_digito_verificador,
    identificacao.sigla as codigo_beneficio_plano_relacionamento,
    nome_modalidade as nome_beneficio_plano_relacionamento,
    nome_modalidade as type,
    identificacao.tipo as codigo_tipo_plano_relacionamento_cliente,
    identificacao.sigla as sigla_sistema,
    identificacao.versao as nome_metodo_pontuacao_plano_relacionamento_cliente,
    concat(agencia_cliente,conta_cliente,digito_conta_cliente)  as codigo_acordo_plano_relacionamento_cliente,
    valor_operacao as valor_produto_plano_relacionamento,
    data_contratacao as data_inicio_vigencia_produto,
    data_vencimento as data_fim_vigencia_produto,
    status_exibicao_contrato as nome_status,
    identificacao.versao as nome_elemento_dado,
    numero as texto_valor_elemento_dado,
    valor_operacao  as valor_total_produto_plano_relacionamento_cliente
FROM view



"""
    
variavel_imoveis = """
SELECT 
cnpj_empresa as numero_cpf,
current_date as data_processamento,
    agencia_cliente as numero_agencia,
    conta_cliente as numero_conta_corrente, 
    digito_conta_cliente as numero_digito_verificador,
    identificacao.sigla as codigo_beneficio_plano_relacionamento,
    nome_modalidade as nome_beneficio_plano_relacionamento,
    nome_modalidade as type,
    identificacao.tipo as codigo_tipo_plano_relacionamento_cliente,
    identificacao.sigla as sigla_sistema,
    identificacao.versao as nome_metodo_pontuacao_plano_relacionamento_cliente,
    concat(agencia_cliente,conta_cliente,digito_conta_cliente)  as codigo_acordo_plano_relacionamento_cliente,
    valor_operacao as valor_produto_plano_relacionamento,
    data_contratacao as data_inicio_vigencia_produto,
    data_vencimento as data_fim_vigencia_produto,
    status_exibicao_contrato as nome_status,
    identificacao.versao as nome_elemento_dado,
    numero as texto_valor_elemento_dado,
    valor_operacao  as valor_total_produto_plano_relacionamento_cliente
FROM view



"""

busca_imoveis = spark.sql(variavel_imoveis)
busca_veiculos = spark.sql(variavel_veiculo)
busca_imoveis.printSchema()
schema = StructType([
    StructField('numero_cpf', StringType(), True),
    StructField('data_processamento', StringType(), True),
    StructField('numero_agencia', StringType(), True),
    StructField('numero_conta_corrente', StringType(), True),
    StructField('numero_digito_verificador', StringType(), True),
    StructField('codigo_beneficio_plano_relacionamento', StringType(), True),
    StructField('nome_beneficio_plano_relacionamento', StringType(), True),
    StructField('type', StringType(), True),
    StructField('codigo_tipo_plano_relacionamento_cliente', StringType(), True),
    StructField('sigla_sistema', StringType(), True),
    StructField('nome_metodo_pontuacao_plano_relacionamento_cliente', StringType(), True),
    StructField('codigo_acordo_plano_relacionamento_cliente', StringType(), True),
    StructField('valor_produto_plano_relacionamento', StringType(), True),
    StructField('data_inicio_vigencia_produto', StringType(), True),
    StructField('data_fim_vigencia_produto', StringType(), True),
    StructField('nome_status', StringType(), True),
    StructField('nome_elemento_dado', StringType(), True),
    StructField('texto_valor_elemento_dado', StringType(), True),
    StructField('valor_total_produto_plano_relacionamento_cliente', StringType(), True),  
])


df = spark.read.json(sc.parallelize(prefix_df), schema, multiLine=True)
df.show()
df.count()

# # for message in busca_imoveis.collect():
# #     added_row_df = spark.createDataFrame(collect_df)

# #salvar para criar tabela,
output_sumarizador= '%s%s%s' % ('s3://pessoajuridicabucket-', 'dess','/plano/financiamento/filtgrho')
from awsglue.dynamicframe import DynamicFrame
dif3 = DynamicFrame.fromDF(df, glueContext, 'test_nest')
datasink= glueContext.write_dynamic_frame.from_options(frame = dif3,\
connection_type = 's3', \
connection_options = {'path': output_sumarizador},
    format='parquet',
transformation_ctx = 'datasink')


