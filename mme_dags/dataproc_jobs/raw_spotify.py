
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import base64
import json, os
from pyspark.sql.types import StringType
from pathlib import Path
from pyspark import StorageLevel
from datetime import datetime

from pyspark_utils import spotify_api
def main_process(api_config):
    spark = SparkSession.builder \
        .appName("raw-spotify-ingestion") \
        .getOrCreate()
    sc = spark.sparkContext
    current_timestamp =  datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #read should be from BQ,this is for local purpose
    internal_df = spark.read.option('header', True).csv('/home/jovyan/work/mme_dags/dataproc_jobs/mme_data.csv')

    internal_df = internal_df.select(F.col('CODE').alias('code'),\
                                    F.col('ORIGINAL ARTIST').alias('original_artist'),\
                                    F.col('SONG TITLE').alias('song_title'))
    internal_df.show()
    clients_cred = f"{api_config['client_id']}:{api_config['client_secret']}"
    credentials = base64.b64encode(clients_cred.encode())
    client = spotify_api(credentials=credentials, token_url=api_config['token_url'], version='v1')
    internal_df = internal_df.repartition(50)

    rdd = internal_df.rdd.persist(StorageLevel.MEMORY_AND_DISK)

    results_rdd = rdd.flatMap(lambda row: client.search(row) or []).filter(lambda x: x is not None)

    if not results_rdd.isEmpty():
        result_df = spark.createDataFrame(results_rdd)
        result_df.show(truncate=False)
    else:
        print("No data retrieved from Spotify API.")

    final_df = result_df.withColumn('ingested_at', F.lit(current_timestamp)).dropDuplicates()
    final_df.printSchema()
    final_df.show()
    final_df.count()

    dir_path = os.path.dirname(os.path.realpath(__file__))
    Path(f"{dir_path}/spotify_data").mkdir(parents=True, exist_ok=True)

    #write should be to BQ and GCS,this is for local purpose

    final_df.write.option("header", True).mode("overwrite").csv(f"{dir_path}/spotify_data/")






if __name__ == '__main__':
    #credentials should be gathered from secret manager
    spotify_config = {
        'client_id' : '',
        'client_secret' : '',
        'token_url':'https://accounts.spotify.com/api/token',
        'search_version': 'v1',
        'bq_table_id':'bronze_layer.l0_spotify'
    }
    main_process(spotify_config)