import os
import json
import glob

input_dir = "../dataset/"
output_file = "../dataset.json"

def collect_json_objects(directory):
    objs = []
    files = glob.glob(os.path.join(directory, '*.json'))
    for fname in files:
        with open(fname, 'r', encoding='utf-8') as f:
            try:
                obj = json.load(f)
                objs.append(obj)
            except Exception as e:
                print(f"Ошибка при чтении {fname}: {e}")
    return objs

if __name__ == "__main__":
    if not os.path.isdir(input_dir):
        print(f"Директория {input_dir} не найдена!")
        exit(1)
    objects = collect_json_objects(input_dir)
    with open(output_file, 'w', encoding='utf-8') as out:
        json.dump(objects, out, ensure_ascii=False, indent=2)
    print(f"Объединено {len(objects)} файлов в {output_file}")
