
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import base64
import json, os
from pathlib import Path
from pyspark import StorageLevel
from datetime import datetime


from pyspark_utils import youtube_api
def main_process(api_config):
    spark = SparkSession.builder \
        .appName("raw-youtube-ingestion") \
        .getOrCreate()
    sc = spark.sparkContext
    internal_df = spark.read.option('header', True).csv('mme_dags/dataproc_jobs/spotify_data/spotify_data.csv')
    internal_df.show()
    internal_df = internal_df.repartition(50)
    current_timestamp =  datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rdd = internal_df.rdd.persist(StorageLevel.MEMORY_AND_DISK)
    results_rdd = rdd.flatMap(lambda row: youtube_api(api_config['api_key'],api_config['search_version'], row) or []).filter(lambda x: x is not None)
    if not results_rdd.isEmpty():
        result_df = spark.createDataFrame(results_rdd)
        result_df.show(truncate=False)
    else:
        print("No data retrieved from Youtube API.")
    final_df = result_df.withColumn('ingested_at', F.lit(current_timestamp)).dropDuplicates()
    final_df.printSchema()
    final_df.show()
    final_df.count()

    dir_path = os.path.dirname(os.path.realpath(__file__))
    Path(f"{dir_path}/youtube_data").mkdir(parents=True, exist_ok=True)

    #write should be to BQ and GCS,this is for local purpose
    final_df.write.option("header", True).mode("overwrite").csv(f"{dir_path}/youtube_data/")


if __name__ == '__main__':
    #credentials should be gathered from secret manager
    spotify_config = {
        'search_version': 'v3',
        'api_key':'',
        'bq_table_id':'bronze_layer.l0_youtube'
    }
    main_process(spotify_config)