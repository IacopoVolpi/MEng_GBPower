import json

notebook_path = r'c:\GBPower\notebooks\IV_flexibility_analysis.ipynb'

# Read the notebook
with open(notebook_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Dictionary of bad character replacements
replacements = {
    'â€"': '–',      # em-dash
    'Ã—': '×',       # multiplication sign  
    'â†'': '→',      # right arrow
    'â†"': '↓',      # down arrow
    'âœ"': '✓',      # checkmark
    'â‰ˆ': '≈',      # approximately equal
    'â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€': '',  # remove decorative dashes
}

original_length = len(content)

# Apply replacements
for bad, good in replacements.items():
    if bad in content:
        count = content.count(bad)
        content = content.replace(bad, good)
        print(f"✓ Replaced '{bad}' → '{good}' ({count} instances)")

# Write back
with open(notebook_path, 'w', encoding='utf-8') as f:
    f.write(content)

new_length = len(content)
print(f"\n✓ File cleaned (size: {original_length} → {new_length} bytes)")
