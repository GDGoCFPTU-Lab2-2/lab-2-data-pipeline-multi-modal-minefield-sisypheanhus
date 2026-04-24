# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

TOXIC_STRINGS = [
    'Null pointer exception',
    'NullPointerException',
    'FATAL ERROR',
    'Segmentation fault',
    'Stack overflow',
    'undefined is not a function',
]


def run_quality_gate(document_dict):
    if not document_dict:
        print("[QA] Rejected: empty document")
        return False

    content = document_dict.get('content', '')

    if len(content) < 20:
        print(f"[QA] Rejected '{document_dict.get('document_id', '?')}': content too short ({len(content)} chars)")
        return False

    for toxic in TOXIC_STRINGS:
        if toxic in content:
            print(f"[QA] Rejected '{document_dict.get('document_id', '?')}': contains toxic string '{toxic}'")
            return False

    metadata = document_dict.get('source_metadata', {})
    discrepancies = metadata.get('discrepancies', [])
    if discrepancies:
        for d in discrepancies:
            print(f"[QA] Warning - discrepancy found: {d}")

    return True
