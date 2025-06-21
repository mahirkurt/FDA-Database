import boto3
import os

# --- Sizin Bilgilerinizle Otomatik Olarak Doldurulmuş Bölüm ---
ACCOUNT_ID = '8343e95913bcf207dcbd3f30dc44c79b'
BUCKET_NAME = 'fda-data-files'
ACCESS_KEY_ID = 'b8f8d21292d0503f054d7f7106c69a39'
SECRET_ACCESS_KEY = '0fb36669f982d5461b25b59bcd0bc9ba589e27eecac5ed9295adc1440cbc22f1'

# Yüklenecek dosyanın adı (bu script ile aynı klasörde olmalı)
SOURCE_FILE_PATH = 'fda_labels.parquet' 
# --- Bilgilerle Doldurulmuş Bölüm Sonu ---


# Kodun geri kalanı bu bilgileri kullanacaktır.
DESTINATION_OBJECT_NAME = os.path.basename(SOURCE_FILE_PATH)
S3_API_ENDPOINT = f'https://{ACCOUNT_ID}.r2.cloudflarestorage.com'

# Boto3 S3 client'ını R2 için yapılandır
try:
    s3 = boto3.client(
        service_name='s3',
        endpoint_url=S3_API_ENDPOINT,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name='auto',
    )
except Exception as e:
    print(f"HATA: Boto3 client oluşturulurken bir sorun oluştu: {e}")
    exit()

print(f"'{SOURCE_FILE_PATH}' dosyası '{BUCKET_NAME}' bucket'ına yükleniyor...")
print("Bu işlem dosya boyutuna bağlı olarak uzun sürebilir, lütfen bekleyin...")

try:
    # Dosyayı yükle
    s3.upload_file(SOURCE_FILE_PATH, BUCKET_NAME, DESTINATION_OBJECT_NAME)
    print("\n--- YÜKLEME BAŞARIYLA TAMAMLANDI! ---")
    
    print(f"\n'{DESTINATION_OBJECT_NAME}' dosyası '{BUCKET_NAME}' isimli R2 bucket'ınıza yüklendi.")
    print("Dosyanın genel erişim URL'sini Cloudflare R2 dashboard'undan kontrol edebilirsiniz.")

except FileNotFoundError:
    print(f"HATA: '{SOURCE_FILE_PATH}' dosyası bulunamadı. Lütfen script'in doğru klasörde olduğundan emin olun.")
except Exception as e:
    print(f"HATA: Yükleme sırasında bir sorun oluştu. Lütfen Access Key ve Secret Key'inizin doğru olduğundan emin olun. Hata detayı: {e}")