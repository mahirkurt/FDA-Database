import pandas as pd
from fastapi import FastAPI, HTTPException

# API uygulamasını oluştur
app = FastAPI(
    title="FDA İlaç Etiketi API",
    description="Bulut tabanlı FDA ilaç verileri üzerinde arama yapan API.",
    version="1.1.0"
)

# Veri dosyasının buluttaki genel adresi
DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev/fda_labels.parquet"

# Performans için, veri setini API başladığında SADECE BİR KEZ belleğe yükle
print(f"Veri seti buluttan yükleniyor: {DATA_URL}")
try:
    df = pd.read_parquet(DATA_URL)
    print("Veri seti başarıyla yüklendi.")
except Exception as e:
    print(f"HATA: Veri seti buluttan yüklenirken bir sorun oluştu: {e}")
    df = None

# Kök endpoint: API'nin çalışıp çalışmadığını test etmek için
@app.get("/")
def read_root():
    return {"mesaj": "Bulut Tabanlı FDA İlaç Veri API'ına hoş geldiniz!"}

# İlaç arama endpoint'i
@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    if df is None:
        raise HTTPException(status_code=500, detail="Sunucu tarafında veri seti yüklenemedi.")

    # Gelen ilaç adının başındaki ve sonundaki boşlukları temizle
    temiz_ilac_adi = ilac_adi.strip()
    
    # İlacı, temizlenmiş adıyla ara
    mask = df['openfda'].astype(str).str.contains(temiz_ilac_adi, na=False, case=False)
    sonuclar = df[mask]
    
    if sonuclar.empty:
        # Hata mesajında da temizlenmiş adı gösterelim
        raise HTTPException(status_code=404, detail=f"'{temiz_ilac_adi}' adında bir ilaç bulunamadı.")

    # Sonuçları JSON formatına uygun güvenli bir sözlüğe çevirip döndür
    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    return sonuclar_guvenli.to_dict(orient='records')