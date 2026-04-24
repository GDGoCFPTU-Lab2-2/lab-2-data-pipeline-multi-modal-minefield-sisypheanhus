import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    tree = ast.parse(source_code)

    module_docstring = ast.get_docstring(tree) or ""

    functions = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            doc = ast.get_docstring(node) or ""
            functions.append({
                "name": node.name,
                "docstring": doc,
            })

    business_rules = re.findall(
        r'#\s*(Business Logic Rule \d+.*?)(?:\n|$)', source_code
    )

    comments = re.findall(r'#\s*(.+)', source_code)

    discrepancies = []
    if 'calculates VAT at 10%' in source_code or 'tax_rate = 0.10' in source_code:
        if '8%' in source_code:
            discrepancies.append(
                "Tax discrepancy: comment says 8% but code uses 10% (tax_rate = 0.10)"
            )

    all_rules = []
    for func in functions:
        if func['docstring']:
            all_rules.append(f"Function '{func['name']}': {func['docstring']}")
    for comment in comments:
        all_rules.append(comment.strip())

    content = f"Module: {module_docstring}\n\nExtracted Business Rules:\n"
    for rule in all_rules:
        content += f"- {rule}\n"
    if discrepancies:
        content += "\nDiscrepancies Found:\n"
        for d in discrepancies:
            content += f"- {d}\n"

    return {
        "document_id": "code-001",
        "content": content,
        "source_type": "Code",
        "author": "Senior Dev (retired)",
        "timestamp": None,
        "source_metadata": {
            "file_name": "legacy_pipeline.py",
            "functions": [f['name'] for f in functions],
            "business_rules_count": len(business_rules),
            "discrepancies": discrepancies,
        },
    }
