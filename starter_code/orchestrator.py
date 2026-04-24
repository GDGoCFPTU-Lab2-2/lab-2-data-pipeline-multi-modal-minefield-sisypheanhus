import json
import time
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")

sys.path.insert(0, SCRIPT_DIR)

from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================


def validate_document(doc_dict):
    try:
        UnifiedDocument(**doc_dict)
        return True
    except Exception as e:
        print(f"[Schema] Validation failed for '{doc_dict.get('document_id', '?')}': {e}")
        return False


def main():
    start_time = time.time()
    final_kb = []

    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")

    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")

    # 1. PDF extraction via Gemini API
    print("--- Processing PDF ---")
    pdf_doc = extract_pdf_data(pdf_path)
    if pdf_doc and run_quality_gate(pdf_doc) and validate_document(pdf_doc):
        final_kb.append(pdf_doc)
        print(f"  Added: {pdf_doc['document_id']}")
    else:
        print("  Skipped PDF (failed quality gate, validation, or API error)")

    # 2. Transcript cleaning
    print("--- Processing Transcript ---")
    trans_doc = clean_transcript(trans_path)
    if trans_doc and run_quality_gate(trans_doc) and validate_document(trans_doc):
        final_kb.append(trans_doc)
        print(f"  Added: {trans_doc['document_id']}")

    # 3. HTML catalog parsing
    print("--- Processing HTML ---")
    html_docs = parse_html_catalog(html_path)
    for doc in html_docs:
        if run_quality_gate(doc) and validate_document(doc):
            final_kb.append(doc)
            print(f"  Added: {doc['document_id']}")

    # 4. CSV sales records
    print("--- Processing CSV ---")
    csv_docs = process_sales_csv(csv_path)
    for doc in csv_docs:
        if run_quality_gate(doc) and validate_document(doc):
            final_kb.append(doc)
            print(f"  Added: {doc['document_id']}")

    # 5. Legacy code business rules
    print("--- Processing Legacy Code ---")
    code_doc = extract_logic_from_code(code_path)
    if code_doc and run_quality_gate(code_doc) and validate_document(code_doc):
        final_kb.append(code_doc)
        print(f"  Added: {code_doc['document_id']}")

    # Save output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_kb, f, indent=2, ensure_ascii=False, default=str)

    end_time = time.time()
    print(f"\nPipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
