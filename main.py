import pandas as pd
import duckdb
from fastapi import FastAPI, HTTPException

# DEĞİŞİKLİK 1: URL'yi yeni, normalize edilmiş dosyamıza yönlendiriyoruz.
DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev/fda_labels_normalized.parquet"

app = FastAPI(
    title="Akıllı FDA API",
    description="Normalize edilmiş FDA verisi üzerinde hızlı sorgu yapan API.",
    version="5.0.0 (Final)"
)

@app.get("/")
def read_root():
    return {"mesaj": "Akıllı ve Verimli FDA İlaç Veri API'ına hoş geldiniz!"}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    print(f"DuckDB ile '{ilac_adi}' için optimize edilmiş arama yapılıyor...")
    
    # Arama terimini sorguya hazır hale getiriyoruz
    temiz_ilac_adi = f"%{ilac_adi.strip().lower()}%"
    
    try:
        con = duckdb.connect()
        
        # DEĞİŞİKLİK 2: Sorguyu artık doğrudan 'brand_name' ve 'generic_name' sütunlarında yapıyoruz.
        # Bu, eski yönteme göre binlerce kat daha hızlıdır.
        query = f"""
            SELECT * FROM '{DATA_URL}' 
            WHERE lower(brand_name) LIKE ? OR lower(generic_name) LIKE ?
        """
        
        sonuclar = con.execute(query, [temiz_ilac_adi, temiz_ilac_adi]).fetchdf()
        
        con.close()

    except Exception as e:
        print(f"HATA: DuckDB sorgusu sırasında bir sorun oluştu: {e}")
        raise HTTPException(status_code=500, detail="Veri sorgulama sırasında bir hata oluştu.")

    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{ilac_adi.strip()}' adında bir ilaç bulunamadı.")

    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    return sonuclar_guvenli.to_dict(orient='records')