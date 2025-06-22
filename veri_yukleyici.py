import pandas as pd
import psycopg2
import io
import time

def upload_data_to_db_professional():
    """
    Pandas to_sql metodunu atlayarak, veriyi psycopg2'nin COPY komutuyla
    doğrudan ve hızlı bir şekilde veritabanına yükler. Bu en sağlam yöntemdir.
    """
    
    # --- Bilgiler ---
    # DATABASE_URL yerine, psycopg2'nin anlayacağı DSN formatını kullanıyoruz.
    DB_CONNECTION_STRING = "dbname='neondb' user='neondb_owner' host='ep-fragrant-cell-a9ahgnn4-pooler.gwc.azure.neon.tech' password='npg_PQ0WeHbfjv1x' sslmode='require'"
    PARQUET_FILE_PATH = 'fda_labels.parquet'
    TABLE_NAME = 'labels'

    # --- Adım A: Veriyi Oku ---
    print("--- Adım A: Yerel Dosya Okunuyor ---")
    try:
        df = pd.read_parquet(PARQUET_FILE_PATH).fillna('')
        # Tüm sütunları metin (string) formatına çevirerek veritabanı uyumluluğunu garantile
        df = df.astype(str)
        print(f"Başarılı: {len(df)} satır veri belleğe yüklendi.")
    except Exception as e:
        print(f"HATA: Parquet dosyası okunurken sorun oluştu: {e}")
        return

    # --- Adım B: Veritabanına Yükleme (Profesyonel Yöntem) ---
    print(f"\n--- Adım B: '{TABLE_NAME}' tablosuna veri yükleme başlatılıyor ---")
    
    conn = None
    try:
        # Veritabanına bağlan
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = conn.cursor()
        print("Veritabanı bağlantısı başarılı.")

        # Eğer tablo varsa sil, sonra yeniden oluştur (temiz başlangıç için)
        print("Eski tablo (varsa) siliniyor ve yenisi oluşturuluyor...")
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        
        # DataFrame'in sütun isimlerinden ve tiplerinden bir CREATE TABLE sorgusu oluştur
        cols = df.columns
        # Tüm sütunları metin olarak oluşturuyoruz (VARCHAR)
        col_definitions = ", ".join([f'"{col}" VARCHAR' for col in cols])
        create_table_query = f"CREATE TABLE {TABLE_NAME} ({col_definitions});"
        cursor.execute(create_table_query)
        print("Yeni tablo başarıyla oluşturuldu.")

        # DataFrame'i CSV formatında bellekte bir dosyaya yaz
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0) # Dosyanın başına geri dön
        
        # PostgreSQL'in en hızlı yükleme metodu olan COPY komutunu kullan
        print("COPY komutu ile hızlı veri aktarımı başlıyor. Bu işlem birkaç dakika sürebilir...")
        start_time = time.time()
        
        cursor.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV HEADER FALSE DELIMITER E'\\t'", buffer)
        
        conn.commit() # Değişiklikleri veritabanına işle

        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n--- VERİ BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam süre: {total_time / 60:.2f} dakika.")

    except Exception as e:
        if conn:
            conn.rollback() # Hata durumunda değişiklikleri geri al
        print(f"HATA: İşlem sırasında bir sorun oluştu: {e}")
    finally:
        if conn:
            conn.close() # Ne olursa olsun bağlantıyı kapat
            print("Veritabanı bağlantısı kapatıldı.")


if __name__ == "__main__":
    upload_data_to_db_professional()