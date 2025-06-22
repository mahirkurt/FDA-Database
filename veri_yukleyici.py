import pandas as pd
import psycopg2
import io
import time

def upload_data_in_chunks():
    """
    Veriyi küçük parçalara (chunk) bölerek belleği verimli kullanır ve
    PostgreSQL'in COPY komutuyla veritabanına yükler.
    """
    
    DB_CONNECTION_STRING = "dbname='neondb' user='neondb_owner' host='ep-fragrant-cell-a9ahgnn4-pooler.gwc.azure.neon.tech' password='npg_PQ0WeHbfjv1x' sslmode='require'"
    PARQUET_FILE_PATH = 'fda_labels.parquet'
    TABLE_NAME = 'labels'
    CHUNKSIZE = 10000  # Her seferinde kaç satır işleneceği

    conn = None
    try:
        print("--- Adım A: Yerel Dosya Okunuyor ---")
        df = pd.read_parquet(PARQUET_FILE_PATH).fillna('')
        df = df.astype(str)
        print(f"Başarılı: {len(df)} satır veri belleğe yüklendi.")

        conn = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = conn.cursor()
        print("Veritabanı bağlantısı başarılı.")

        print(f"\n--- Adım B: '{TABLE_NAME}' tablosu oluşturuluyor ---")
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        cols = df.columns
        col_definitions = ", ".join([f'"{col}" VARCHAR' for col in cols])
        create_table_query = f"CREATE TABLE {TABLE_NAME} ({col_definitions});"
        cursor.execute(create_table_query)
        print("Yeni tablo başarıyla oluşturuldu.")
        
        print(f"\n--- Adım C: Veri {CHUNKSIZE} satırlık parçalar halinde yükleniyor ---")
        start_time = time.time()
        
        # --- YENİ MANTIĞIMIZ BURADA (CHUNKING) ---
        for i in range(0, len(df), CHUNKSIZE):
            chunk = df.iloc[i:i+CHUNKSIZE]
            
            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False, sep='\t')
            buffer.seek(0)
            
            cursor.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV HEADER FALSE DELIMITER E'\\t'", buffer)
            
            # İlerleme durumu bildir
            print(f"  {min(i + CHUNKSIZE, len(df))} / {len(df)} satır yüklendi...")

        conn.commit()

        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n--- VERİ BAŞARIYLA YÜKLENDİ! ---")
        print(f"Toplam süre: {total_time / 60:.2f} dakika.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"HATA: İşlem sırasında bir sorun oluştu: {e}")
    finally:
        if conn:
            conn.close()
            print("Veritabanı bağlantısı kapatıldı.")

if __name__ == "__main__":
    upload_data_in_chunks()