import json
import re

def scan_json_for_pattern(filename, regex_pattern, window=50):
    # Загрузить json (обычно это dict: id -> {text: ..., ...})
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Компилировать паттерн (пример: r'\bIC\s*[-_]?50\b')
    pattern = re.compile(regex_pattern, re.IGNORECASE)

    results = []
    for doc_id, doc in data.items():
        text = doc['text'] if isinstance(doc, dict) and 'text' in doc else str(doc)
        for m in pattern.finditer(text):
            start = max(0, m.start() - window)
            end = min(len(text), m.end() + window)
            excerpt = text[start:end]
            results.append({
                'doc_id': doc_id,
                'match': m.group(0),
                'start': m.start(),
                'end': m.end(),
                'excerpt': excerpt
            })

    return results

# Пример использования:
# regex_pat = r'\bIC\s*[-_]?50\b|\bEC\s*[-_]?50\b|\bK[ _\-]?i\b|\bK[ _\-]?d\b'
# Можно делать read_json_and_scan_for_constants('myfile.json', regex_pat)
if __name__ == '__main__':
    import sys
    # Например: python scan_json.py data.json
    filename = input("Введите имя вашего json-файла: ").strip()
    # Пример паттерна (расширяйте под задачу!)
    # Парсер v3
    sub_5 = "₅"
    sub_0 = "₀"
    digits = r'(?:50|₅₀)'

    pattern = (
        r'(?:'
        r'\bIC' + digits + r'\s*\(\s*nM\s*\)'
        r'|\bEC' + digits + r'\s*\(\s*nM\s*\)'
        r'|\bKi\s*\(\s*nM\s*\)'
        r'|\bKd\s*\(\s*nM\s*\)'
        r')'
    )
    regex = re.compile(pattern)
    results = scan_json_for_pattern(filename, regex_pat)

    for res in results:
        print(f"Doc: {res['doc_id']} | Found: {res['match']} | [{res['start']}-{res['end']}]")
        print(f"...{res['excerpt']}...")
        print('-' * 70)

    print(f'Всего найдено совпадений: {len(results)}')