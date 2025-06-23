from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine # 'text' import'unu kaldırdık
import pandas as pd
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

app = FastAPI(title="Orange Book API", version="2.1.0 (Final-Fix)")

@app.get("/")
def read_root():
    return {"mesaj": "FDA Orange Book API'ına hoş geldiniz!", "veritabanı_durumu": "Bağlandı" if engine is not None else "Bağlantı Hatası"}

@app.get("/orangebook/arama")
def search_orange_book(arama_terimi: str):
    if engine is None:
        raise HTTPException(status_code=500, detail="Veritabanı bağlantısı yapılandırılamadı.")

    # --- DEĞİŞİKLİK BURADA ---
    # Sorguyu artık basit bir metin (string) olarak tanımlıyoruz.
    # Güvenli parametre stili olarak :search_term yerine %(search_term)s kullanıyoruz.
    query = """
        SELECT * FROM orange_book_products 
        WHERE "Drug_Name" ILIKE %(search_term)s OR "Active_Ingredient" ILIKE %(search_term)s
        LIMIT 100;
    """
    
    try:
        with engine.connect() as connection:
            # params dictionary'si bu yeni stile uygun şekilde çalışır.
            sonuclar = pd.read_sql(query, connection, params={"search_term": f"%{arama_terimi}%"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sorgu sırasında bir hata oluştu: {e}")
    
    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{arama_terimi}' için sonuç bulunamadı.")

    return sonuclar.to_dict(orient='records')