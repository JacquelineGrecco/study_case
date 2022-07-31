from pyspark.sql import SparkSession
import pyspark.sql.functions as f 
from utils.SparkConfig import SparkConfig
import os


if __name__ == '__main__':

    spark = SparkConfig()
    
    data_format = 'csv'


    data_load_path = '../study_case/data/data_usuario.csv'
    data_write_path = '../study_case/output_data/data_ordem/'

    user_load_path = '../study_case/data/usuario.csv'
    data_user_load_path = '../study_case/data/data_usuario.csv'

    df_data = spark.read.format(data_format).option('header', True).option('sep', '|').option('encoding','UTF-8').load(data_load_path)
    df_user = spark.read.format(data_format).option('header', True).option('sep', '|').option('encoding','ISO-8859-1').load(user_load_path)
    df_data_user = spark.read.format(data_format).option('header', True).option('sep', ';').option('encoding','ISO-8859-1').load(data_user_load_path)

    df_final_user_data = df_data_user.join(df_user, df_data_user.USUARIO == df_user.ID, how='left')
    df_final_user_data = df_data_user.groupBy('USUARIO').count().select(f.col('count'), 'USUARIO')
    df_final_user_data.coalesce(1).write.format(data_format).mode('overwrite').option('header', True).option('sep', '|').option('encoding','UTF-8').save(data_write_path)
    os.system("cat ../study_case/output_data/data_order/part-00000-tid-9072036691794417551-5a9b0eb9-cfbe-42b8-8c8b-6fa40562fff9-26-1-c000.csv > ../study_case/output_data/data_order/cont_usuario.csv")