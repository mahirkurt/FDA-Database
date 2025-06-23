from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os

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

app = FastAPI(title="Orange Book API", version="10.0 (Final-Production)")

@app.get("/")
def read_root():
    return {"mesaj": "FDA Orange Book API'ına hoş geldiniz!", "veritabanı_durumu": "Bağlandı" if engine is not None else "Bağlantı Hatası"}

@app.get("/orangebook/arama")
def search_orange_book(arama_terimi: str):
    if engine is None:
        raise HTTPException(status_code=500, detail="Veritabanı bağlantısı yapılandırılamadı.")

    arama_parametresi = f"%{arama_terimi.strip().lower()}%"
    
    # --- SON DEĞİŞİKLİK BURADA: Sütun adları artık çift tırnak içinde ---
    query = text("""
        SELECT * FROM orange_book_products 
        WHERE "trade_name" ILIKE :search_term OR "ingredient" ILIKE :search_term
        LIMIT 100;
    """)
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"search_term": arama_parametresi})
            sonuclar = [dict(row._mapping) for row in result]
            
    except Exception as e:
        print(f"HATA: Veritabanı sorgusu sırasında bir sorun oluştu: {e}")
        raise HTTPException(status_code=500, detail=f"Sorgu sırasında bir hata oluştu: {e}")
    
    if not sonuclar:
        raise HTTPException(status_code=404, detail=f"'{arama_terimi}' için sonuç bulunamadı.")

    return sonuclar