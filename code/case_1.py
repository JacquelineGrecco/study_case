from pyspark.sql import SparkSession
import pyspark.sql.functions as f 
from pyspark.sql.window import Window
from utils.SparkConfig import SparkConfig
import os

if __name__ == '__main__':

    spark = SparkConfig()
    
    data_format = 'csv'

    data_load_path = '../study_case/data/data_usuario.csv'
    data_write_path = '../study_case/output_data/data_ordem/'

    
    df_data = spark.read.format(data_format).option('header', True).option('sep', '|').option('encoding','UTF-8').load(data_load_path)
    df_data = df_data.orderBy('TRANSACAO')

    # CREATING THE INDEX COLUMN WITH WINDOW FUNC
    df_data = df_data.withColumn('idx', f.monotonically_increasing_id())
    window = Window.orderBy('idx')
    df_data_index = df_data.withColumn('ORDEM', f.row_number().over(window)).drop('idx')
    
    df_data_index = df_data_index.orderBy(['MUNICIPIO', 'DATA_ATUALIZACAO', 'TRANSACAO'], ascending=True)
    df_final_data = df_data_index.withColumnRenamed('ORDEM', 'ORDEM_ORIGINAL')
    
    cols_order = ['TRANSACAO', 'MUNICIPIO', 'DATA_ATUALIZACAO', 'ORDEM_ORIGINAL']
    
    df_final_data = df_final_data.select(cols_order)
    df_final_data.coalesce(1).write.format(data_format).mode('overwrite').option('header', True).option('sep', '|').option('encoding','UTF-8').save(data_write_path)
    os.system("cat ../study_case/output_data/data_ordem/part-00000-tid-6285181532927111650-533aefb6-f596-4fe1-9d5d-8061cf18a951-36-1-c000.csv > ../study_case/output_data/data_order/data_ordem.csv")
