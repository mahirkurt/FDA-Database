import pandas as pd
import duckdb
from fastapi import FastAPI, HTTPException
import string # Harf kontrolü için

# R2 bucket'ınızın temel URL'si
BASE_DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev"

app = FastAPI(
    title="Akıllı Bölümlenmiş FDA API",
    description="Parçalanmış veri üzerinde sorgu yapan API.",
    version="6.0.0 (Final)"
)

@app.get("/")
def read_root():
    return {"mesaj": "Parçalanmış Veri API'ına hoş geldiniz!"}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    ilac_adi_temiz = ilac_adi.strip().lower()
    
    if not ilac_adi_temiz:
        raise HTTPException(status_code=400, detail="İlaç adı boş olamaz.")

    # Aranan ilacın ilk harfini al
    ilk_harf = ilac_adi_temiz[0]
    
    # Sadece ingiliz alfabesi harfleri için devam et
    if ilk_harf not in string.ascii_lowercase:
        raise HTTPException(status_code=400, detail="Arama sadece harfle başlayabilir.")

    # Sorgulanacak spesifik dosyanın tam URL'sini oluştur
    DATA_URL = f"{BASE_DATA_URL}/labels_{ilk_harf}.parquet"
    
    print(f"'{ilac_adi}' için arama yapılıyor, hedef dosya: {DATA_URL}")
    
    arama_terimi = f"%{ilac_adi_temiz}%"
    
    try:
        con = duckdb.connect()
        
        # DuckDB'ye URL'deki Parquet dosyasını okumasını söylüyoruz
        # read_parquet(), URL'leri doğrudan okuyabilir
        query = """
            SELECT * FROM read_parquet(?) 
            WHERE lower(brand_name) LIKE ? OR lower(generic_name) LIKE ?
        """
        
        sonuclar = con.execute(query, [DATA_URL, arama_terimi, arama_terimi]).fetchdf()
        con.close()
        
    except Exception as e:
        # DuckDB'nin dosyayı bulamadığı durumlar için özel hata yönetimi
        # R2'de o harf için bir dosya yoksa, DuckDB 404 hatası verir.
        if "404 Not Found" in str(e):
             raise HTTPException(status_code=404, detail=f"'{ilac_adi.strip()}' adıyla başlayan ilaçlar için veri bulunamadı.")
        
        print(f"HATA: DuckDB sorgusu sırasında bir sorun oluştu: {e}")
        raise HTTPException(status_code=500, detail="Veri sorgulama sırasında bir hata oluştu.")

    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{ilac_adi.strip()}' adında bir ilaç bulunamadı.")

    # Sonuçları güvenli bir formata çevirip döndür
    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    return sonuclar_guvenli.to_dict(orient='records')