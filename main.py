import pandas as pd
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# --- Veri Yükleme ve API "Yaşam Döngüsü" ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Bu bölüm, uygulama BAŞLAMADAN HEMEN ÖNCE çalışır.
    print("Uygulama başlıyor...")
    
    DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev/fda_labels.parquet"
    print(f"Veri seti buluttan yükleniyor: {DATA_URL}")
    
    try:
        # Veri setini yükle ve uygulamanın "state"ine ekle
        app.state.df = pd.read_parquet(DATA_URL)
        print("Veri seti başarıyla yüklendi ve kullanıma hazır.")
    except Exception as e:
        print(f"KRİTİK HATA: Veri seti yüklenemedi: {e}")
        app.state.df = None
    
    yield  # yield'den sonra uygulama gelen istekleri kabul etmeye başlar
    
    # Bu bölüm, uygulama KAPANDIĞINDA çalışır (temizlik için).
    print("Uygulama kapanıyor...")


# --- API Uygulaması ---

# FastAPI uygulamasını, yukarıda tanımladığımız yaşam döngüsü ile oluştur
app = FastAPI(
    lifespan=lifespan,
    title="FDA İlaç Etiketi API",
    description="Bulut tabanlı FDA ilaç verileri üzerinde arama yapan API.",
    version="1.2.0"
)

@app.get("/")
def read_root():
    return {"mesaj": "Bulut Tabanlı FDA İlaç Veri API'ına hoş geldiniz! Veri seti durumu: " + ("Yüklendi" if app.state.df is not None else "Yüklenemedi")}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    # Veri setine artık app.state.df üzerinden erişiyoruz
    if app.state.df is None:
        raise HTTPException(status_code=500, detail="Sunucu tarafında veri seti yüklenemedi veya hala yükleniyor.")

    temiz_ilac_adi = ilac_adi.strip()
    
    mask = app.state.df['openfda'].astype(str).str.contains(temiz_ilac_adi, na=False, case=False)
    sonuclar = app.state.df[mask]
    
    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{temiz_ilac_adi}' adında bir ilaç bulunamadı.")

    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    return sonuclar_guvenli.to_dict(orient='records')