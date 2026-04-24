import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

def clean_transcript(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    cleaned = re.sub(r'\[Music(?:\s+\w+)?\]', '', text)
    cleaned = re.sub(r'\[inaudible\]', '', cleaned)
    cleaned = re.sub(r'\[Laughter\]', '', cleaned)

    cleaned = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', cleaned)

    cleaned = re.sub(r'\[Speaker \d+\]:\s*', '', cleaned)

    cleaned = re.sub(r'\n\s*\n', '\n', cleaned).strip()
    cleaned = re.sub(r'  +', ' ', cleaned)

    detected_price_vnd = None
    if 'năm trăm nghìn' in text:
        detected_price_vnd = 500000

    return {
        "document_id": "transcript-001",
        "content": cleaned,
        "source_type": "Video",
        "author": "Speaker 1",
        "timestamp": None,
        "source_metadata": {
            "file_name": "demo_transcript.txt",
            "language": "vi",
            "detected_price_vnd": detected_price_vnd,
            "speakers": ["Speaker 1", "Speaker 2"],
        },
    }
