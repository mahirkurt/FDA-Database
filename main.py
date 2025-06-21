import pandas as pd
import requests
import io
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# --- Veri Yükleme ve API "Yaşam Döngüsü" ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Uygulama başlıyor...")
    
    DATA_URL = "https://pub-e1e1e9482d2e4295b8f9dada0d679f0a.r2.dev/fda_labels.parquet"
    
    # Cloudflare'in bot korumasını aşmak için kendimizi tarayıcı gibi tanıtıyoruz
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"Veri seti buluttan yükleniyor: {DATA_URL}")
    
    try:
        # requests kütüphanesi ile dosyayı indiriyoruz
        response = requests.get(DATA_URL, headers=headers)
        response.raise_for_status()  # Eğer 4xx veya 5xx hata kodu varsa, burada hata fırlatır

        # İndirilen içeriği bellekteki bir dosyaya dönüştürüp pandas'a veriyoruz
        parquet_file = io.BytesIO(response.content)
        app.state.df = pd.read_parquet(parquet_file)
        
        print("Veri seti başarıyla yüklendi ve kullanıma hazır.")
    except Exception as e:
        print(f"KRİTİK HATA: Veri seti yüklenemedi: {e}")
        app.state.df = None
    
    yield
    
    print("Uygulama kapanıyor...")


# --- API Uygulaması ---

app = FastAPI(
    lifespan=lifespan,
    title="FDA İlaç Etiketi API",
    description="Bulut tabanlı FDA ilaç verileri üzerinde arama yapan API.",
    version="1.3.0" # versiyonu güncelledik
)

# ... (API'nın geri kalan kodları aynı kalacak, onlarda değişiklik yok) ...

@app.get("/")
def read_root():
    return {"mesaj": "Bulut Tabanlı FDA İlaç Veri API'ına hoş geldiniz! Veri seti durumu: " + ("Yüklendi" if app.state.df is not None else "Yüklenemedi")}

@app.get("/ilac/{ilac_adi}")
def get_ilac_bilgisi(ilac_adi: str):
    if app.state.df is None:
        raise HTTPException(status_code=500, detail="Sunucu tarafında veri seti yüklenemedi veya hala yükleniyor.")

    temiz_ilac_adi = ilac_adi.strip()
    
    mask = app.state.df['openfda'].astype(str).str.contains(temiz_ilac_adi, na=False, case=False)
    sonuclar = app.state.df[mask]
    
    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{temiz_ilac_adi}' adında bir ilaç bulunamadı.")

    sonuclar_guvenli = sonuclar.fillna('').astype(str)
    return sonuclar_guvenli.to_dict(orient='records')