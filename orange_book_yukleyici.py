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
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(ORANGE_BOOK_URL, headers=headers)
        response.raise_for_status()

        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        
        with zip_file.open('products.txt', 'r') as f:
            col_names = ['Ingredient', 'DF_Route', 'Trade_Name', 'Applicant', 'Strength', 'Appl_Type_No', 'Product_No', 'TE_Code', 'Approval_Date', 'RLD', 'RS', 'Type', 'Applicant_Full_Name']
            df = pd.read_csv(io.TextIOWrapper(f, 'latin1'), sep='~', names=col_names, header=None).fillna('')
            print("Veri başarıyla okundu.")

        # --- YENİ EKLENEN SATIR ---
        # Veritabanına göndermeden önce TÜM sütun adlarını küçük harfe çeviriyoruz.
        df.columns = [col.lower() for col in df.columns]
        print("Sütun adları küçük harfe çevrildi.")
        # --- DEĞİŞİKLİK SONU ---

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