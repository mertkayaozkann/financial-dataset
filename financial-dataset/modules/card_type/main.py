from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def analyze_card_brand_and_type(data_path):
    # Spark oturumunu başlat
    spark = SparkSession.builder \
        .appName("Card Brand and Type Analysis") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Veri yolları
    cards_data_path = f"/shared-data/cards_data.csv"

    # Cards Data dosyasını yükle
    cards_data_df = spark.read.csv(cards_data_path, header=True, inferSchema=True)

    # Kolon adlarını kontrol et
    print("Columns in cards data:", cards_data_df.columns)

    # Kart markası ve kart türüne göre işlem sayısını hesapla
    card_brand_and_type = cards_data_df.groupBy("card_brand", "card_type").agg(
        count("card_brand").alias("count")
    ).orderBy("count", ascending=False)

    # Sonuçları tablo şeklinde göster
    print("Card Brand and Type Usage Analysis:")
    card_brand_and_type.show()

    # Spark oturumunu kapat
    spark.stop()

if __name__ == "__main__":
    analyze_card_brand_and_type("/home/mertkayaozkan/shared-data")










