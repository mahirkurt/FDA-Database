import pandas as pd
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
import string

BASE_DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev"

app = FastAPI(
    title="Akıllı Bölümlenmiş FDA API",
    description="Parçalanmış veri üzerinde sorgu yapan API.",
    version="8.0.0 (Production-Ready)"
)

@app.get("/")
def read_root():
    return {"mesaj": "Parçalanmış Veri API'ına hoş geldiniz!"}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    print(f"DuckDB ile '{ilac_adi}' için optimize edilmiş arama yapılıyor...")
    
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
        query = f"""
            SELECT * FROM read_parquet('{DATA_URL_PARTITION}') 
            WHERE lower(CAST(brand_name AS VARCHAR)) LIKE ? OR lower(CAST(generic_name AS VARCHAR)) LIKE ?
        """
        sonuclar = con.execute(query, [arama_terimi, arama_terimi]).fetchdf()
        con.close()
    except Exception as e:
        if "404 Not Found" in str(e):
             raise HTTPException(status_code=404, detail=f"'{ilac_adi.strip()}' adıyla başlayan ilaçlar için veri bulunamadı.")
        print(f"HATA: DuckDB sorgusu sırasında bir sorun oluştu: {e}")
        raise HTTPException(status_code=500, detail="Veri sorgulama sırasında bir hata oluştu.")

    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{ilac_adi.strip()}' adında bir ilaç bulunamadı.")

    # --- NİHAİ ÇÖZÜM BURADA ---
    # .fillna('').astype(str) gibi genel bir temizlik yerine,
    # FastAPI'nin kendi akıllı dönüştürücüsünü kullanıyoruz.
    # Bu dönüştürücü, NaN gibi değerleri JSON standardına uygun olan 'null' değerine
    # otomatik olarak ve hatasız bir şekilde çevirir.
    json_uyumlu_sonuclar = jsonable_encoder(sonuclar.to_dict(orient='records'))
    return json_uyumlu_sonuclar