from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None

app = FastAPI(title="Orange Book API", version="3.0.0 (Corrected Schema)")

@app.get("/")
def read_root():
    return {"mesaj": "FDA Orange Book API'ına hoş geldiniz!", "veritabanı_durumu": "Bağlandı" if engine is not None else "Bağlantı Hatası"}

@app.get("/orangebook/arama")
def search_orange_book(arama_terimi: str):
    if engine is None:
        raise HTTPException(status_code=500, detail="Veritabanı bağlantısı yapılandırılamadı.")

    # --- DEĞİŞİKLİK BURADA: Doğru sütun adlarını sorguluyoruz ---
    query = text("""
        SELECT * FROM orange_book_products 
        WHERE "Trade_Name" ILIKE :search_term OR "Ingredient" ILIKE :search_term
        LIMIT 100;
    """)
    
    try:
        with engine.connect() as connection:
            sonuclar = pd.read_sql(query, connection, params={"search_term": f"%{arama_terimi}%"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sorgu sırasında bir hata oluştu: {e}")
    
    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{arama_terimi}' için sonuç bulunamadı.")

    return sonuclar.to_dict(orient='records')