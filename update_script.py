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
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(ORANGE_BOOK_URL, headers=headers)
    response.raise_for_status()

    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    
    with zip_file.open('products.txt', 'r') as f:
        col_names = [
            'ingredient', 'df_route', 'trade_name', 'applicant', 'strength', 
            'appl_type_no', 'product_no', 'te_code', 'approval_date', 
            'rld', 'rs', 'type', 'applicant_full_name'
        ]
        # Veriyi okurken tüm sütunları metin olarak oku ve boş değerleri temizle
        df = pd.read_csv(io.TextIOWrapper(f, 'latin1'), sep='~', names=col_names, header=None, dtype=str).fillna('')
        print("Data processed successfully.")

    # Temizlenmiş veriyi CSV olarak kaydet
    df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"Data saved to '{OUTPUT_CSV_PATH}'. Total rows: {len(df)}")

if __name__ == "__main__":
    fetch_and_save_orange_book()