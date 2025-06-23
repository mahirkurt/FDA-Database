import requests
import zipfile
import io
import pandas as pd

ORANGE_BOOK_URL = "https://www.fda.gov/media/76860/download?attachment"
OUTPUT_CSV_PATH = "products.csv" # Projenin ana klasörüne kaydedilecek

def fetch_and_save_orange_book():
    """
    Orange Book verisini indirir ve CSV olarak kaydeder.
    """
    print("Downloading Orange Book data...")

    # --- DEĞİŞİKLİK BURADA ---
    # Kendimizi normal bir tarayıcı olarak tanıtmak için headers ekliyoruz
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    # isteği bu başlıklarla birlikte yapıyoruz
    response = requests.get(ORANGE_BOOK_URL, headers=headers)
    # --- DEĞİŞİKLİK SONU ---

    # Hata varsa (4xx veya 5xx) programı durdurur
    response.raise_for_status()

    print("Download successful. Processing data...")
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    
    with zip_file.open('products.txt', 'r') as f:
        # Sütun adlarını küçük harfe çevirerek standartlaştırıyoruz
        col_names = [
            'ingredient', 'df_route', 'trade_name', 'applicant', 'strength', 
            'appl_type_no', 'product_no', 'te_code', 'approval_date', 
            'rld', 'rs', 'type', 'applicant_full_name'
        ]
        # Veriyi okurken tüm sütunları metin olarak oku ve boş değerleri temizle
        df = pd.read_csv(io.TextIOWrapper(f, 'latin1'), sep='~', names=col_names, header=None, dtype=str).fillna('')

    # Temizlenmiş veriyi CSV olarak kaydet
    df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"Data saved to '{OUTPUT_CSV_PATH}'. Total rows: {len(df)}")

if __name__ == "__main__":
    fetch_and_save_orange_book()
