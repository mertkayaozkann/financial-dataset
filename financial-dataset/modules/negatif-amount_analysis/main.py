from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

def negatif_amount_analysis(data_path):
    # Spark oturumunu başlat
    spark = SparkSession.builder \
        .appName("Negative Balance Analysis") \
        .getOrCreate()

    # Dosya yolları
    transactions_path = f"/shared-data/transactions_data.csv"

    # Transactions veri setini yükle
    transactions_df = spark.read.csv(transactions_path, header=True, inferSchema=True)

    # Amount sütunundaki $ sembolünü kaldır ve sayıya çevir
    transactions_df = transactions_df.withColumn('amount',
                                                 regexp_replace('amount', '[$,]', '').cast('double'))

    # Negatif bakiye olan işlemleri filtrele
    negative_balance_df = transactions_df.filter(col('amount') < 0)

    # Sonuçları göster
    negative_balance_df.select('client_id', 'card_id', 'amount').show()

    # Spark oturumunu kapat
    spark.stop()

# Fonksiyonu çağır
negatif_amount_analysis("/home/mertkayaozkan/shared-data")

