from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd
import os

# Render'da ayarlayacağımız ortam değişkeninden veritabanı adresini alacak
DATABASE_URL = os.environ.get("DATABASE_URL")

engine = None
# Sadece DATABASE_URL tanımlıysa veritabanı motorunu oluştur
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL)
        print("Veritabanı bağlantı motoru başarıyla oluşturuldu.")
    except Exception as e:
        print(f"Veritabanı bağlantısı kurulamadı: {e}")
else:
    print("KRİTİK HATA: DATABASE_URL ortam değişkeni bulunamadı. Lütfen Render ayarlarınızı kontrol edin.")

# API uygulamasını oluştur
app = FastAPI(title="FDA Orange Book API", version="2.0.0 (Database-Powered)")

@app.get("/")
def read_root():
    return {"mesaj": "FDA Orange Book API'ına hoş geldiniz!", "veritabanı_durumu": "Bağlandı" if engine is not None else "Bağlantı Hatası"}

@app.get("/orangebook/arama")
def search_orange_book(arama_terimi: str):
    if engine is None:
        raise HTTPException(status_code=500, detail="Veritabanı bağlantısı yapılandırılamadı.")

    # ILIKE, büyük/küçük harf duyarsız arama yapar
    # LIMIT 100 ile çok fazla sonuç dönmesini engelliyoruz
    query = text("""
        SELECT * FROM orange_book_products 
        WHERE "Drug_Name" ILIKE :search_term OR "Active_Ingredient" ILIKE :search_term
        LIMIT 100;
    """)
    
    try:
        with engine.connect() as connection:
            # params ile güvenli sorgulama yapıyoruz
            sonuclar = pd.read_sql(query, connection, params={"search_term": f"%{arama_terimi}%"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sorgu sırasında bir hata oluştu: {e}")

    
    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{arama_terimi}' için sonuç bulunamadı.")

    return sonuclar.to_dict(orient='records')