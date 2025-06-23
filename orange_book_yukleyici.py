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
    try:
        print("Orange Book verisi indiriliyor...")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(ORANGE_BOOK_URL, headers=headers)
        response.raise_for_status()

        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        
        with zip_file.open('products.txt', 'r') as f:
            # --- DEĞİŞİKLİK BURADA: FDA'in resmi dokümantasyonundaki doğru sütun adları ---
            col_names = [
                'Ingredient', 'DF_Route', 'Trade_Name', 'Applicant', 'Strength', 
                'Appl_Type_No', 'Product_No', 'TE_Code', 'Approval_Date', 
                'RLD', 'RS', 'Type', 'Applicant_Full_Name'
            ]
            df = pd.read_csv(io.TextIOWrapper(f, 'latin1'), sep='~', names=col_names, header=None).fillna('')
            print("Veri başarıyla okundu ve doğru sütunlarla DataFrame'e dönüştürüldü.")

        engine = create_engine(DATABASE_URL)
        print(f"\n'{TABLE_NAME}' tablosuna doğru şema ile veri yükleniyor...")
        start_time = time.time()
        
        # if_exists='replace' ile eski, hatalı tabloyu silip yenisini oluşturacak
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        
        end_time = time.time()
        print(f"--- VERİ BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam {len(df)} satır, {(end_time - start_time):.2f} saniyede yüklendi.")

    except Exception as e:
        print(f"HATA: İşlem sırasında bir sorun oluştu: {e}")

if __name__ == "__main__":
    update_orange_book_data()