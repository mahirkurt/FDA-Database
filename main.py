from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None

app = FastAPI(title="Orange Book API", version="Final")

@app.get("/")
def read_root():
    return {"mesaj": "FDA Orange Book API'ına hoş geldiniz!", "veritabanı_durumu": "Bağlandı" if engine is not None else "Bağlantı Hatası"}

@app.get("/orangebook/arama")
def search_orange_book(arama_terimi: str):
    if engine is None:
        raise HTTPException(status_code=500, detail="Veritabanı bağlantısı yapılandırılamadı.")

    arama_parametresi = f"%{arama_terimi.strip().lower()}%"
    
    # --- DEĞİŞİKLİK BURADA: Sütun adları artık küçük harfli ve tırnaksız ---
    query = text("""
        SELECT * FROM orange_book_products 
        WHERE trade_name ILIKE :search_term OR ingredient ILIKE :search_term
        LIMIT 100;
    """)
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"search_term": arama_parametresi})
            sonuclar = [dict(row._mapping) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sorgu sırasında bir hata oluştu: {e}")
    
    if not sonuclar:
        raise HTTPException(status_code=404, detail=f"'{arama_terimi}' için sonuç bulunamadı.")

    return sonuclar