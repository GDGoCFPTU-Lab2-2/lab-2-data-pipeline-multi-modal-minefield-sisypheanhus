from google import genai
from google.genai import types
import json
import os
import time

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Use Gemini API to extract structured data from lecture_notes.pdf

def extract_pdf_data(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY not set")
        return None

    client = genai.Client(api_key=api_key)

    with open(file_path, "rb") as f:
        pdf_bytes = f.read()

    pdf_part = types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")

    prompt = (
        "Extract structured metadata from this PDF document. "
        "Return a JSON object with these fields:\n"
        '- "title": the document title or type\n'
        '- "author": the author or issuing organization\n'
        '- "summary": a concise summary of the content\n'
        '- "tables": any structured/tabular data as a list of objects\n'
        '- "date": the document date if present (ISO format)\n'
        "Return ONLY valid JSON, no markdown fences."
    )

    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=[prompt, pdf_part],
            )
            raw_text = response.text.strip()
            if raw_text.startswith("```"):
                raw_text = raw_text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            try:
                parsed = json.loads(raw_text)
            except json.JSONDecodeError:
                parsed = {}

            author = parsed.get("author", "Unknown")
            title = parsed.get("title", "Unknown Document")
            summary = parsed.get("summary", raw_text)
            doc_date = parsed.get("date")
            tables = parsed.get("tables", [])

            content = f"Title: {title}\nSummary: {summary}"
            if tables:
                content += f"\nTables: {json.dumps(tables, ensure_ascii=False)}"

            return {
                "document_id": "pdf-001",
                "content": content,
                "source_type": "PDF",
                "author": author,
                "timestamp": doc_date,
                "source_metadata": {
                    "file_name": os.path.basename(file_path),
                    "title": title,
                    "tables": tables,
                    "raw_response": raw_text[:500],
                },
            }
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                wait = 2 ** attempt
                print(f"Rate limited. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"Gemini API error: {e}")
                return None

    print("Max retries exceeded for Gemini API.")
    return None
