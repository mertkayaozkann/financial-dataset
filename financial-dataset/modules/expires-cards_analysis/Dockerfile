# Apache Spark'ın Python destekli Bitnami imajını kullanıyoruz
FROM bitnami/spark:3.3.0

# Çalışma dizinini ayarla
WORKDIR /app

# Gerekli dosyaları konteynıra kopyala
COPY . /app

# Python bağımlılıklarını yükle
RUN pip install --no-cache-dir -r requirements.txt

# Spark'ı başlatmak için gerekli ortam değişkenlerini ayarla
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Ana Python dosyasını çalıştır
CMD ["python", "main.py"]