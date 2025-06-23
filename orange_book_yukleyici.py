import requests
import zipfile
import io
import pandas as pd
from sqlalchemy import create_engine
import time

# --- Yapılandırma ---
ORANGE_BOOK_URL = "https://www.fda.gov/media/76860/download"
TABLE_NAME = 'orange_book_products'
DATABASE_URL = "postgresql://neondb_owner:npg_PQ0WeHbfjv1x@ep-fragrant-cell-a9ahgnn4-pooler.gwc.azure.neon.tech/neondb?sslmode=require"

def update_orange_book_data():
    """
    FDA'in sitesinden en güncel Orange Book verisini indirir, işler
    ve PostgreSQL veritabanına yükler.
    """
    try:
        # --- Adım 1: Veriyi İndirme ve Açma ---
        print("Orange Book verisi indiriliyor...")

        # --- DEĞİŞİKLİK BURADA ---
        # Kendimizi normal bir tarayıcı olarak tanıtmak için headers ekliyoruz
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # isteği bu başlıklarla birlikte yapıyoruz
        response = requests.get(ORANGE_BOOK_URL, headers=headers)
        # --- DEĞİŞİKLİK SONU ---
        
        response.raise_for_status()

        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        
        with zip_file.open('products.txt', 'r') as f:
            col_names = ['Appl_Type', 'Appl_No', 'Product_No', 'Form', 'Strength', 'Reference_Drug', 'Drug_Name', 'Active_Ingredient', 'Submission_Status']
            df = pd.read_csv(io.TextIOWrapper(f, 'latin1'), sep='~', names=col_names, header=None).fillna('')
            print("Veri başarıyla okundu ve DataFrame'e dönüştürüldü.")

        # --- Adım 2: Veriyi Veritabanına Yükleme ---
        engine = create_engine(DATABASE_URL)
        print(f"\n'{TABLE_NAME}' tablosuna veri yükleniyor...")
        start_time = time.time()
        
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        
        end_time = time.time()
        print(f"--- VERİ BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam {len(df)} satır, {(end_time - start_time):.2f} saniyede yüklendi.")

    except Exception as e:
        print(f"HATA: İşlem sırasında bir sorun oluştu: {e}")

if __name__ == "__main__":
    update_orange_book_data()