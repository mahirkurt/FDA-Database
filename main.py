import pandas as pd
from fastapi import FastAPI, HTTPException

# API uygulamasını oluştur
app = FastAPI(
    title="FDA İlaç Etiketi API",
    description="Lokal FDA ilaç verileri üzerinde arama yapan API.",
    version="1.0.0"
)

# Performans için, veri setini API başladığında SADECE BİR KEZ belleğe yükle
print("Veri seti 'fda_labels.parquet' dosyasından yükleniyor...")
try:
    df = pd.read_parquet('fda_labels.parquet')
    print("Veri seti başarıyla yüklendi.")
except FileNotFoundError:
    print("HATA: 'fda_labels.parquet' dosyası bulunamadı. Lütfen main.py ile aynı klasörde olduğundan emin olun.")
    df = None

@app.get("/")
def read_root():
    return {"mesaj": "FDA İlaç Veri API'ına hoş geldiniz!"}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    if df is None:
        raise HTTPException(status_code=500, detail="Sunucu tarafında veri seti yüklenemedi.")

    temiz_ilac_adi = ilac_adi.strip()
    
    mask = df['openfda'].astype(str).str.contains(temiz_ilac_adi, na=False, case=False)
    sonuclar = df[mask]
    
    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{temiz_ilac_adi}' adında bir ilaç bulunamadı.")

    # --- ÇÖZÜM BURADA ---
    # JSON'a çevirmeden önce, olası uyumsuz tipleri önlemek için
    # tüm sonuçları güvenli bir şekilde string'e çevirelim.
    # .fillna('') ile olası NaN (boş) değerlerini de boş metne çeviriyoruz.
    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    
    # Artık bu "güvenli" DataFrame'i sözlüğe çeviriyoruz
    return sonuclar_guvenli.to_dict(orient='records')