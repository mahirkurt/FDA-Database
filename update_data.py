import requests
import zipfile
import io
import pandas as pd
import ijson
import os

# FDA'in toplu indirme verilerinin bulunduğu ana URL
DOWNLOAD_URL = "https://download.open.fda.gov/drug/label/drug-label-0001-of-0001.json.zip" # Bu URL dinamik olarak bulunabilir, şimdilik sabit varsayalım

def fetch_and_process_data():
    print("En güncel veriler FDA'den çekiliyor...")
    # 1. En güncel zip dosyasını indir
    response = requests.get(DOWNLOAD_URL)
    response.raise_for_status()  # Hata varsa programı durdur

    # 2. İndirilen zip dosyasını bellekte aç
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    
    # 3. Zip içindeki JSON dosyasını bul ve işle
    for filename in zip_file.namelist():
        if filename.endswith('.json'):
            print(f"{filename} işleniyor...")
            with zip_file.open(filename) as f:
                # Daha önce yazdığımız ijson mantığı ile veriyi DataFrame'e çevir
                objects = ijson.items(f, 'results.item')
                df = pd.DataFrame(objects)
                
                # 4. Yeni Parquet dosyasını kaydet
                df.to_parquet('fda_labels_latest.parquet')
                print("Yeni veri başarıyla 'fda_labels_latest.parquet' dosyasına kaydedildi.")
                
                # 5. (Opsiyonel) Eski dosyayı yenisiyle değiştir
                # os.replace('fda_labels_latest.parquet', 'fda_labels.parquet')
                return

if __name__ == "__main__":
    fetch_and_process_data()