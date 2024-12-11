from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def state_city_analysis(data_path):
    # Spark oturumunu başlat
    spark = SparkSession.builder \
        .appName("State City Analysis") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Veri yolu
    transactions_path = f"/shared-data/transactions_data.csv"

    # Veri setini yükle
    transactions_df = spark.read.csv(transactions_path, header=True, inferSchema=True)

    # Merchant City ve Merchant State'e göre grup oluştur, her şehirdeki işlem sayısını hesapla
    city_state_count_df = transactions_df.groupBy("merchant_state", "merchant_city").agg(
        count("id").alias("transaction_count")
    )

    # Sonuçları göster
    city_state_count_df.show(truncate=False)

    # Spark oturumunu kapat
    spark.stop()

if __name__ == "__main__":
    state_city_analysis("/home/mertkayaozkan/shared-data")
