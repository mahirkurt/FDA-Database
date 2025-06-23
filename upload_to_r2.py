import boto3
import os
import glob
import time

# --- Yapılandırma ---
# Lütfen bu bilgilerin doğruluğunu kontrol edin.
ACCOUNT_ID = '8343e95913bcf207dcbd3f30dc44c79b'
BUCKET_NAME = 'fda-data-files'
ACCESS_KEY_ID = 'b8f8d21292d0503f054d7f7106c69a39' 
SECRET_ACCESS_KEY = '0fb36669f982d5461b25b59bcd0bc9ba589e27eecac5ed9295adc1440cbc22f1' 

# Yüklenecek dosyaların bulunduğu klasörün adı
SOURCE_DIRECTORY = 'partitioned_data'


# --- Kod Başlangıcı ---
S3_API_ENDPOINT = f'https://{ACCOUNT_ID}.r2.cloudflarestorage.com'

try:
    s3 = boto3.client(
        service_name='s3',
        endpoint_url=S3_API_ENDPOINT,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name='auto',
    )
    
    # Kaynak klasördeki tüm .parquet dosyalarını bul
    search_pattern = os.path.join(SOURCE_DIRECTORY, '*.parquet')
    files_to_upload = glob.glob(search_pattern)
    
    if not files_to_upload:
        print(f"HATA: '{SOURCE_DIRECTORY}' klasöründe yüklenecek .parquet dosyası bulunamadı.")
    else:
        print(f"Toplam {len(files_to_upload)} dosya yüklenecek. Başlatılıyor...")
        start_time = time.time()
        
        # Her bir dosyayı yüklemek için döngü
        for i, file_path in enumerate(files_to_upload):
            # R2'deki dosya adı, sadece dosyanın kendi adı olacak (klasör olmadan)
            object_name = os.path.basename(file_path)
            print(f"  {i+1}/{len(files_to_upload)}: '{object_name}' yükleniyor...")
            
            s3.upload_file(file_path, BUCKET_NAME, object_name)
        
        end_time = time.time()
        print(f"\n--- TÜM DOSYALAR BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam süre: {(end_time - start_time):.2f} saniye.")

except Exception as e:
    print(f"HATA: Yükleme sırasında bir sorun oluştu: {e}")