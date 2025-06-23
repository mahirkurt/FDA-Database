import pandas as pd
import duckdb
from fastapi import FastAPI, HTTPException
from typing import Optional
import string

BASE_DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev"

app = FastAPI(
    title="Sayfalamalı Akıllı FDA API",
    description="Parçalanmış veri üzerinde sayfalama destekli sorgu yapan API.",
    version="7.0.0 (Production Ready)"
)

@app.get("/")
def read_root():
    return {"mesaj": "Sayfalamalı Akıllı FDA API'ına hoş geldiniz!"}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str, skip: int = 0, limit: int = 1): # YENİ PARAMETRELER
    print(f"DuckDB ile '{ilac_adi}' için arama yapılıyor (skip={skip}, limit={limit})...")
    
    ilac_adi_temiz = ilac_adi.strip().lower()
    
    if not ilac_adi_temiz:
        raise HTTPException(status_code=400, detail="İlaç adı boş olamaz.")

    ilk_harf = ilac_adi_temiz[0]
    if ilk_harf not in string.ascii_lowercase:
        raise HTTPException(status_code=400, detail="Arama sadece harfle başlayabilir.")

    DATA_URL_PARTITION = f"{BASE_DATA_URL}/labels_{ilk_harf}.parquet"
    arama_terimi = f"%{ilac_adi_temiz}%"
    
    try:
        con = duckdb.connect()
        # --- DEĞİŞİKLİK BURADA: LIMIT ve OFFSET eklendi ---
        query = f"""
            SELECT * FROM read_parquet('{DATA_URL_PARTITION}') 
            WHERE lower(brand_name) LIKE ? OR lower(generic_name) LIKE ?
            LIMIT ? OFFSET ?
        """
        sonuclar = con.execute(query, [arama_terimi, arama_terimi, limit, skip]).fetchdf()
        con.close()
    except Exception as e:
        # ... (hata yönetimi aynı kalacak) ...
        print(f"HATA: DuckDB sorgusu sırasında bir sorun oluştu: {e}")
        raise HTTPException(status_code=500, detail="Veri sorgulama sırasında bir hata oluştu.")

    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{ilac_adi.strip()}' adında bir ilaç bulunamadı.")

    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    return sonuclar_guvenli.to_dict(orient='records')