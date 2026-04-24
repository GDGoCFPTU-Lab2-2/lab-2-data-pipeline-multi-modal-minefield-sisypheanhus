from bs4 import BeautifulSoup

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def parse_html_catalog(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table = soup.find('table', id='main-catalog')
    if not table:
        print("Error: Could not find table with id 'main-catalog'")
        return []

    rows = table.find('tbody').find_all('tr')
    products = []

    for i, row in enumerate(rows):
        cols = [td.get_text(strip=True) for td in row.find_all('td')]
        if len(cols) < 6:
            continue

        product_id, name, category, price_str, stock_str, rating = cols

        price = None
        if price_str not in ('N/A', 'Liên hệ'):
            cleaned = price_str.replace(',', '').replace('VND', '').strip()
            try:
                price = float(cleaned)
            except ValueError:
                price = None

        try:
            stock = int(stock_str)
        except ValueError:
            stock = 0

        products.append({
            "document_id": f"html-{product_id}",
            "content": f"Product: {name}, Category: {category}, Price: {price_str}, Stock: {stock}, Rating: {rating}",
            "source_type": "HTML",
            "author": "VinShop",
            "timestamp": None,
            "source_metadata": {
                "product_id": product_id,
                "product_name": name,
                "category": category,
                "price_vnd": price,
                "stock": stock,
                "rating": rating,
            },
        })

    return products
