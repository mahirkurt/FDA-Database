import pandas as pd
import time
from sqlalchemy import create_engine

def upload_data_to_db():
    """
    Yerel Parquet dosyasını okur ve buluttaki PostgreSQL veritabanına yükler.
    """
    
    DATABASE_URL = "postgresql://neondb_owner:npg_PQ0WeHbfjv1x@ep-fragrant-cell-a9ahgnn4-pooler.gwc.azure.neon.tech/neondb?sslmode=require"
    PARQUET_FILE_PATH = 'fda_labels.parquet'

    print("--- Adım A: Yerel Dosya Okunuyor ---")
    try:
        df = pd.read_parquet(PARQUET_FILE_PATH)
        print(f"Başarılı: {len(df)} satır veri belleğe yüklendi.")
    except Exception as e:
        print(f"HATA: Parquet dosyası okunurken sorun oluştu: {e}")
        return

    print("\n--- Adım B: Veritabanına Yükleme Başlatılıyor ---")
    
    try:
        engine = create_engine(DATABASE_URL)
        print("SQLAlchemy motoru oluşturuldu.")
        
        print("Veritabanı bağlantısı kuruluyor ve veri aktarılıyor... BU İŞLEM ÇOK UZUN SÜREBİLİR...")
        start_time = time.time()
        
        # 'engine' nesnesini kullanarak bir 'connection' (bağlantı) bloğu açıyoruz
        # ve to_sql'e bu 'connection' nesnesini veriyoruz.
        with engine.connect() as connection:
            df.to_sql(
                name='labels',
                con=connection,  # 'engine' yerine 'connection' kullanılıyor
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