import json
import re
import sys

def scan_json_for_pattern(filename, regex_pattern, window=50):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    pattern = re.compile(regex_pattern, re.IGNORECASE)
    results = []

    for ext_id, doc in data.items():
        cur_id = ext_id
        text = None

        if isinstance(doc, str):
            text = doc

        elif isinstance(doc, dict):
            cur_id = doc.get('id', ext_id)
            if 'text' in doc and isinstance(doc['text'], str):
                text = doc['text']
            else:
                for v in doc.values():
                    if isinstance(v, str) and len(v) > 30:
                        text = v
                        break

        if not isinstance(text, str):
            continue

        for m in pattern.finditer(text):
            start = max(0, m.start() - window)
            end = min(len(text), m.end() + window)
            excerpt = text[start:end]
            results.append({
                'doc_id': cur_id,
                'match': m.group(0),
                'start': m.start(),
                'end': m.end(),
                'excerpt': excerpt
            })
    return results

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <json_file> <regex_pattern>")
        sys.exit(1)

    filename = sys.argv[1]
    regex_pat = sys.argv[2]

    results = scan_json_for_pattern(filename, regex_pat)
    for res in results:
        print(f"Doc: {res['doc_id']} | Found: {res['match']} | [{res['start']}-{res['end']}]")
        print(f"...{res['excerpt']}...")
        print('-' * 70)

    print(f'Всего найдено совпадений: {len(results)}')
