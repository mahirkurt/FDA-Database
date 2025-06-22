import pandas as pd
import psycopg2
import io
import time

def upload_data_to_db_professional():
    """
    Bu fonksiyon, yerel bir Parquet dosyasını okur, veriyi küçük parçalara (chunk)
    bölerek belleği verimli kullanır ve PostgreSQL'in en hızlı yükleme metodu
    olan COPY komutuyla veriyi buluttaki veritabanına aktarır.
    Bu, projenin en sağlam ve nihai veri yükleme script'idir.
    """
    
    # --- Yapılandırma Bilgileri ---
    DB_CONNECTION_STRING = "dbname='neondb' user='neondb_owner' host='ep-fragrant-cell-a9ahgnn4-pooler.gwc.azure.neon.tech' password='npg_PQ0WeHbfjv1x' sslmode='require'"
    PARQUET_FILE_PATH = 'fda_labels.parquet'
    TABLE_NAME = 'labels'
    CHUNKSIZE = 10000  # Her seferinde kaç satır işleneceği

    conn = None
    try:
        # --- Adım A: Yerel Dosyayı Oku ve Hazırla ---
        print("--- Adım A: Yerel Dosya Okunuyor ---")
        # fillna('') ile olası boş değerleri temizle, astype(str) ile tüm veriyi metne çevirerek uyumluluğu garantile
        df = pd.read_parquet(PARQUET_FILE_PATH).fillna('')
        df = df.astype(str)
        print(f"Başarılı: {len(df)} satır veri belleğe yüklendi.")

        # --- Adım B: Veritabanı Bağlantısı ve Tablo Hazırlığı ---
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = conn.cursor()
        print("Veritabanı bağlantısı başarılı.")

        print(f"\n--- Adım B: '{TABLE_NAME}' tablosu oluşturuluyor ---")
        # Eğer tablo varsa sil, sonra yeniden oluştur (temiz başlangıç için)
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        
        # DataFrame'in sütun isimlerinden ve tiplerinden bir CREATE TABLE sorgusu oluştur
        # Hata riskini en aza indirmek için tüm sütunları metin olarak (VARCHAR) oluşturuyoruz.
        cols = df.columns
        col_definitions = ", ".join([f'"{col}" VARCHAR' for col in cols])
        create_table_query = f"CREATE TABLE {TABLE_NAME} ({col_definitions});"
        cursor.execute(create_table_query)
        print("Yeni tablo başarıyla oluşturuldu.")
        
        # --- Adım C: Veriyi Parçalar Halinde Yükleme ---
        print(f"\n--- Adım C: Veri {CHUNKSIZE} satırlık parçalar halinde yükleniyor ---")
        start_time = time.time()
        
        # DataFrame'i CHUNKSIZE ile belirtilen boyutlarda parçalara böl ve döngüye al
        for i in range(0, len(df), CHUNKSIZE):
            chunk = df.iloc[i:i+CHUNKSIZE]
            
            # Her bir parçayı bellekte geçici bir CSV dosyasına çevir
            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False, sep=',')
            buffer.seek(0)
            
            # PostgreSQL'in en hızlı yükleme metodu olan COPY komutunu kullan
            copy_sql = f"COPY {TABLE_NAME} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
            cursor.copy_expert(copy_sql, buffer)
            
            # Kullanıcıya ilerleme durumu hakkında bilgi ver
            print(f"  {min(i + CHUNKSIZE, len(df))} / {len(df)} satır yüklendi...")

        # Tüm parçalar yüklendikten sonra değişiklikleri veritabanına kalıcı olarak işle
        conn.commit()

        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n--- VERİ BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam süre: {total_time / 60:.2f} dakika.")

    except Exception as e:
        # Hata durumunda yapılan tüm değişiklikleri geri al
        if conn:
            conn.rollback()
        print(f"HATA: İşlem sırasında bir sorun oluştu: {e}")
    finally:
        # Ne olursa olsun, işlem bitince veritabanı bağlantısını kapat
        if conn:
            conn.close()
            print("Veritabanı bağlantısı kapatıldı.")


# Script doğrudan çalıştırıldığında bu fonksiyonu çağır
if __name__ == "__main__":
    upload_data_to_db_professional()