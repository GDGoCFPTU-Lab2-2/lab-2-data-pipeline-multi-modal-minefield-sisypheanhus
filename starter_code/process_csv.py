import pandas as pd
from dateutil import parser as date_parser

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

WORD_TO_NUM = {
    'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4,
    'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9,
    'ten': 10, 'twenty': 20, 'thirty': 30, 'forty': 40, 'fifty': 50,
    'hundred': 100, 'thousand': 1000,
}


def parse_price(val):
    if pd.isna(val) or str(val).strip().upper() in ('N/A', 'NULL', 'Liên hệ', ''):
        return None
    val_str = str(val).strip()
    val_str = val_str.replace('$', '').replace(',', '').strip()
    try:
        result = float(val_str)
        if result < 0:
            return None
        return result
    except ValueError:
        pass
    words = val_str.lower().split()
    if all(w in WORD_TO_NUM or w == 'dollars' for w in words):
        total = sum(WORD_TO_NUM.get(w, 0) for w in words if w != 'dollars')
        return float(total) if total > 0 else None
    return None


def normalize_date(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        dt = date_parser.parse(str(val), dayfirst=False)
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def process_sales_csv(file_path):
    df = pd.read_csv(file_path)

    df = df.drop_duplicates(subset='id', keep='first')

    df['price_clean'] = df['price'].apply(parse_price)
    df['date_clean'] = df['date_of_sale'].apply(normalize_date)

    results = []
    for _, row in df.iterrows():
        results.append({
            "document_id": f"csv-{int(row['id'])}",
            "content": f"Sale: {row['product_name']}, Price: {row['price_clean']}, Currency: {row['currency']}, Date: {row['date_clean']}, Seller: {row['seller_id']}",
            "source_type": "CSV",
            "author": str(row['seller_id']),
            "timestamp": row['date_clean'],
            "source_metadata": {
                "product_name": row['product_name'],
                "category": row['category'],
                "price": row['price_clean'],
                "currency": row['currency'],
                "date_of_sale": row['date_clean'],
                "seller_id": row['seller_id'],
                "stock_quantity": row['stock_quantity'] if pd.notna(row.get('stock_quantity')) else None,
            },
        })

    return results
