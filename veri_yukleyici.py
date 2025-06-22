# veri_yukleyici.py

import pandas as pd
import time
# Bu script doğrudan çalışacağı için, SQLAlchemy'nin kurulu olması gerekir.
# Hata durumunda aşağıdaki hata mesajı bunu teyit edecektir.

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
        print(f"HATA: '{PARQUET_FILE_PATH}' dosyası bulunamadı. Lütfen bu script ile aynı klasörde olduğundan emin olun.")
        return # Hata varsa programdan çık
    except Exception as e:
        print(f"HATA: Parquet dosyası okunurken sorun oluştu: {e}")
        return

    # --- Adım B: Veritabanına Yükle ---
    print("\n--- Adım B: Veritabanına Yükleme Başlatılıyor ---")
    print("BU İŞLEM ÇOK UZUN SÜREBİLİR (15-30+ DAKİKA). LÜTFEN SABIRLA BEKLEYİN...")
    
    start_time = time.time()
    try:
        df.to_sql(
            name='labels',
            con=DATABASE_URL,
            if_exists='replace',
            index=False,
            chunksize=1000,
            method='multi'
        )
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n--- VERİ BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam süre: {total_time / 60:.2f} dakika.")
        
    except ImportError as e:
        print(f"\nKRİTİK HATA: '{e}'. Bu hatayı alıyorsanız, sqlalchemy veya psycopg2-binary kütüphanesi kurulu değildir.")
        print("Lütfen Komut Satırı'nı açıp 'pip install \"sqlalchemy<2.0\" \"psycopg2-binary\"' komutunu çalıştırın.")
    except Exception as e:
        print(f"HATA: Veritabanına yükleme sırasında bir sorun oluştu: {e}")


if __name__ == "__main__":
    upload_data_to_db()