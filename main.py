import pandas as pd
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# --- Uygulama Başlangıcında Veriyi Yükleme ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global df_orange_book
    print("Uygulama başlıyor, Orange Book verisi yükleniyor...")
    try:
        # Kodla aynı yerde olan CSV dosyasını oku
        df_orange_book = pd.read_csv("products.csv", dtype=str).fillna('')
        print(f"Veri başarıyla yüklendi. {len(df_orange_book)} satır.")
    except FileNotFoundError:
        print("UYARI: products.csv dosyası bulunamadı. API boş veri ile çalışacak.")
        df_orange_book = pd.DataFrame()
    yield
    print("Uygulama kapanıyor.")

app = FastAPI(lifespan=lifespan, title="Basit Orange Book API", version="1.0")

@app.get("/search")
def search_orange_book(query: str):
    if df_orange_book.empty:
        raise HTTPException(status_code=503, detail="Veri seti şu anda mevcut değil.")

    # Küçük harfe çevirerek arama yap
    search_term = query.strip().lower()
    
    # Hem marka adında hem de etken maddede ara
    mask = (
        df_orange_book['trade_name'].str.lower().str.contains(search_term, na=False) |
        df_orange_book['ingredient'].str.lower().str.contains(search_term, na=False)
    )
    
    sonuclar = df_orange_book[mask]

    if sonuclar.empty:
        raise HTTPException(status_code=404, detail=f"'{query}' için sonuç bulunamadı.")
        
    return sonuclar.to_dict(orient='records')