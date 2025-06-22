import pandas as pd
import time
from sqlalchemy import create_engine # KÜTÜPHANEYİ MANUEL OLARAK IMPORT EDİYORUZ

def upload_data_to_db():
    """
    Yerel Parquet dosyasını okur ve buluttaki PostgreSQL veritabanına yükler.
    """
    
    # --- Bilgiler ---
    DATABASE_URL = "postgresql://neondb_owner:npg_PQ0WeHbfjv1x@ep-fragrant-cell-a9ahgnn4-pooler.gwc.azure.neon.tech/neondb?sslmode=require"
    PARQUET_FILE_PATH = 'fda_labels.parquet'

    # --- Adım A: Veriyi Oku ---
    print("--- Adım A: Yerel Dosya Okunuyor ---")
    try:
        df = pd.read_parquet(PARQUET_FILE_PATH)
        print(f"Başarılı: {len(df)} satır veri belleğe yüklendi.")
    except FileNotFoundError:
        print(f"HATA: '{PARQUET_FILE_PATH}' dosyası bulunamadı.")
        return
    except Exception as e:
        print(f"HATA: Parquet dosyası okunurken sorun oluştu: {e}")
        return

    # --- Adım B: Veritabanına Yükleme ---
    print("\n--- Adım B: Veritabanına Yükleme Başlatılıyor ---")
    
    try:
        # Pandas'a URL vermek yerine, SQLAlchemy motorunu biz oluşturuyoruz.
        print("SQLAlchemy motoru oluşturuluyor...")
        engine = create_engine(DATABASE_URL)
        print("Motor oluşturuldu.")

        print("Veri veritabanına aktarılıyor... BU İŞLEM ÇOK UZUN SÜREBİLİR...")
        start_time = time.time()
        
        # to_sql fonksiyonuna, URL yerine, bizim oluşturduğumuz 'engine' nesnesini veriyoruz.
        df.to_sql(
            name='labels',
            con=engine, 
            if_exists='replace',
            index=False,
            chunksize=1000,
            method='multi'
        )

        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n--- VERİ BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam süre: {total_time / 60:.2f} dakika.")
        
    except Exception as e:
        print(f"HATA: Veritabanına yükleme sırasında bir sorun oluştu: {e}")


if __name__ == "__main__":
    upload_data_to_db()