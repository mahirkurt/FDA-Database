from sqlalchemy import create_engine, text

# Neon veritabanı bağlantı adresiniz
DATABASE_URL = "postgresql://neondb_owner:npg_PQ0WeHbfjv1x@ep-fragrant-cell-a9ahgnn4-pooler.gwc.azure.neon.tech/neondb?sslmode=require"
TABLE_NAME = 'orange_book_products'

def optimize_database():
    """
    PostgreSQL veritabanına bağlanır, pg_trgm eklentisini aktif eder
    ve arama yapılacak sütunlar için GIN indeksleri oluşturur.
    Bu, LIKE ve ILIKE sorgularını binlerce kat hızlandırır.
    """
    print("Veritabanına bağlanılıyor...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            trans = connection.begin()
            try:
                # 1. pg_trgm eklentisini aktif et
                print("pg_trgm eklentisi aktif ediliyor...")
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
                print("Eklenti aktif.")

                # 2. Drug_Name sütunu için GIN indeksi oluştur
                print(f'"Drug_Name" sütunu için indeks oluşturuluyor... (Bu işlem biraz sürebilir)')
                connection.execute(text(f'CREATE INDEX idx_{TABLE_NAME}_drug_name_trgm ON {TABLE_NAME} USING gin ("Drug_Name" gin_trgm_ops);'))
                print("Drug_Name indeksi başarıyla oluşturuldu.")
                
                # 3. Active_Ingredient sütunu için GIN indeksi oluştur
                print(f'"Active_Ingredient" sütunu için indeks oluşturuluyor...')
                connection.execute(text(f'CREATE INDEX idx_{TABLE_NAME}_active_ingredient_trgm ON {TABLE_NAME} USING gin ("Active_Ingredient" gin_trgm_ops);'))
                print("Active_Ingredient indeksi başarıyla oluşturuldu.")
                
                trans.commit() # Tüm işlemleri onayla
                print("\n--- VERİTABANI BAŞARIYLA OPTİMİZE EDİLDİ! ---")
                
            except Exception as e_inner:
                print(f"HATA: İndeksleme sırasında bir sorun oluştu: {e_inner}")
                trans.rollback()

    except Exception as e_outer:
        print(f"HATA: Veritabanı bağlantısında bir sorun oluştu: {e_outer}")

if __name__ == "__main__":
    optimize_database()