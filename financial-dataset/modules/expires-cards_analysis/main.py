from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date

# Spark oturumunu başlat
spark = SparkSession.builder \
    .appName("Expired Cards Analysis") \
    .getOrCreate()

# Dosya yolu
cards_data_path = f"/shared-data/cards_data.csv"

# Kart verisini yükle
cards_df = spark.read.csv(cards_data_path, header=True, inferSchema=True)

# 'expires' sütununu tarih formatına dönüştür
cards_df = cards_df.withColumn('expires', to_date('expires', 'MM/yyyy'))

# Güncel tarihe göre süresi geçmiş kartları bul
expired_cards_df = cards_df.filter(col('expires') < current_date())

# Süresi geçmiş kartların bilgilerini yazdır
print("Expired Cards Analysis:")
expired_cards_df.select('client_id', 'card_number', 'expires').show()

# Spark oturumunu kapat
spark.stop()
