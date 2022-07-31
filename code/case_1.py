from pyspark.sql import SparkSession
import pyspark.sql.functions as f 
from pyspark.sql.window import Window
from utils.SparkConfig import SparkConfig


if __name__ == '__main__':

    spark = SparkConfig()
    
    data_format = 'csv'
    data_load_path = 'dbfs:/FileStore/data.csv'
    data_write_path = 'dbfs:/FileStore/data_order.csv'
    
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
