import boto3
import os

# --- Sizin Bilgilerinizle Doldurulmuş Bölüm ---
ACCOUNT_ID = '8343e95913bcf207dcbd3f30dc44c79b'
BUCKET_NAME = 'fda-data-files'
ACCESS_KEY_ID = 'b8f8d21292d0503f054d7f7106c69a39' # Lütfen kendi güncel anahtarınızı kullanın
SECRET_ACCESS_KEY = '0fb36669f982d5461b25b59bcd0bc9ba589e27eecac5ed9295adc1440cbc22f1' # Lütfen kendi güncel anahtarınızı kullanın

# --- DEĞİŞİKLİK BURADA ---
# Yüklenecek dosyanın adını yeni, normalize edilmiş dosya olarak değiştiriyoruz.
SOURCE_FILE_PATH = 'fda_labels_normalized.parquet' 
# --- DEĞİŞİKLİK SONU ---

DESTINATION_OBJECT_NAME = os.path.basename(SOURCE_FILE_PATH)
S3_API_ENDPOINT = f'https://{ACCOUNT_ID}.r2.cloudflarestorage.com'

try:
    s3 = boto3.client(
        service_name='s3',
        endpoint_url=S3_API_ENDPOINT,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name='auto',
    )
    print(f"'{SOURCE_FILE_PATH}' dosyası '{BUCKET_NAME}' bucket'ına yükleniyor...")
    print("Bu işlem dosya boyutuna bağlı olarak uzun sürebilir, lütfen bekleyin...")
    s3.upload_file(SOURCE_FILE_PATH, BUCKET_NAME, DESTINATION_OBJECT_NAME)
    print("\n--- YÜKLEME BAŞARIYLA TAMAMLANDI! ---")
except Exception as e:
    print(f"HATA: Yükleme sırasında bir sorun oluştu: {e}")