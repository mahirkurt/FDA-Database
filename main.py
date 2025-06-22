import pandas as pd
import duckdb
from fastapi import FastAPI, HTTPException

# Veri dosyasının buluttaki genel adresi
DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev/fda_labels.parquet"

app = FastAPI(
    title="DuckDB Destekli FDA API",
    description="Uzak Parquet dosyası üzerinde sorgu çalıştıran API.",
    version="3.0.0"
)

@app.get("/")
def read_root():
    return {"mesaj": "DuckDB Destekli FDA İlaç Veri API'ına hoş geldiniz!"}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    print(f"DuckDB ile '{ilac_adi}' için arama yapılıyor...")
    
    temiz_ilac_adi = ilac_adi.strip()
    
    try:
        # DuckDB'ye bağlan (dosya adı olmadan, bellekte çalışır)
        con = duckdb.connect()
        
        # Doğrudan URL üzerindeki Parquet dosyasında SQL sorgusu çalıştır!
        # '?' ile güvenli parametre kullanımı sağlıyoruz.
        query = f"""
            SELECT * FROM '{DATA_URL}' 
            WHERE lower(openfda) LIKE ?
        """
        
        # Sorguyu çalıştır ve sonucu bir Pandas DataFrame'e çevir
        sonuclar = con.execute(query, [f"%{temiz_ilac_adi.lower()}%"]).fetchdf()
        
        con.close()

    except Exception as e:
        print(f"HATA: DuckDB sorgusu sırasında bir sorun oluştu: {e}")
        raise HTTPException(status_code=500, detail="Veri sorgulama sırasında bir hata oluştu.")

    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{temiz_ilac_adi}' adında bir ilaç bulunamadı.")

    # Sonuçları güvenli bir formata çevirip döndür
    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    return sonuclar_guvenli.to_dict(orient='records')