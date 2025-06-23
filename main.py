from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os

# Render'daki ortam değişkenlerinden veritabanı adresini alacak
DATABASE_URL = os.environ.get("DATABASE_URL")
engine = None

if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL)
        print("Veritabanı bağlantı motoru başarıyla oluşturuldu.")
    except Exception as e:
        print(f"Veritabanı bağlantısı kurulamadı: {e}")
else:
    print("KRİTİK HATA: DATABASE_URL ortam değişkeni bulunamadı.")

app = FastAPI(title="Doğrudan Veritabanı API", version="9.0.0 (Direct-DB)")

@app.get("/")
def read_root():
    return {"mesaj": "Doğrudan Veritabanı FDA API'ına hoş geldiniz!", "veritabanı_durumu": "Bağlandı" if engine is not None else "Bağlantı Hatası"}

@app.get("/orangebook/arama")
def search_orange_book(arama_terimi: str):
    if engine is None:
        raise HTTPException(status_code=500, detail="Veritabanı bağlantısı yapılandırılamadı.")

    # Arama terimini SQL LIKE sorgusu için hazırla
    arama_parametresi = f"%{arama_terimi.strip()}%"
    
    # Güvenli sorgulama için 'text' ve parametre bağlama kullanıyoruz
    query = text("""
        SELECT * FROM orange_book_products 
        WHERE "Drug_Name" ILIKE :search_term OR "Active_Ingredient" ILIKE :search_term
        LIMIT 100;
    """)
    
    try:
        with engine.connect() as connection:
            # --- DEĞİŞİKLİK BURADA: Artık pandas.read_sql KULLANMIYORUZ ---
            # Sorguyu doğrudan SQLAlchemy ile çalıştırıyoruz
            result = connection.execute(query, {"search_term": arama_parametresi})
            # Gelen sonuçları bir sözlük listesine çeviriyoruz
            sonuclar = [dict(row._mapping) for row in result]
            
    except Exception as e:
        print(f"HATA: Veritabanı sorgusu sırasında bir sorun oluştu: {e}")
        raise HTTPException(status_code=500, detail=f"Sorgu sırasında bir hata oluştu: {e}")
    
    if not sonuclar:
        raise HTTPException(status_code=404, detail=f"'{arama_terimi}' için sonuç bulunamadı.")

    return sonuclar