import os
import json
import glob
from math import ceil

input_dirs = [
    "../google_parser/output/output_v1",
    "../google_parser/output/output_v2",
    "../google_parser/output/output_v3",
    "../google_parser/output/output_v4_parser_v2",
    "../google_parser/output/output_v5_parser_v2"
]
out_dir = "../dataset/"
n_shards = 25

# =============================

def collect_json_objects(dirs):
    objects = []
    for d in dirs:
        files = glob.glob(os.path.join(d, '*.json'))
        for fname in files:
            with open(fname, encoding='utf-8') as f:
                try:
                    obj = json.load(f)
                    objects.append(obj)
                except Exception as e:
                    print(f"Ошибка при чтении {fname}: {e}")
    return objects

def estimate_obj_size(obj):
    return len(json.dumps(obj, ensure_ascii=False).encode('utf-8')) + 2  # Запас под запятые и формат

def shard_by_size(objects, n_shards):
    sizes = [estimate_obj_size(obj) for obj in objects]
    total = sum(sizes)
    target = total // n_shards

    shards = [[] for _ in range(n_shards)]
    shard_sizes = [0 for _ in range(n_shards)]
    i = 0
    for obj, size in sorted(zip(objects, sizes), key=lambda x: -x[1]):  # От больших к меньшим, для баланса
        # Кладём в самый "маленький" шард из оставшихся
        idx = shard_sizes.index(min(shard_sizes))
        shards[idx].append(obj)
        shard_sizes[idx] += size
    return shards, shard_sizes

def save_shards(shards, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    results = []
    for i, shard in enumerate(shards):
        fname = os.path.join(out_dir, f"shard_{i+1:03}.json")
        with open(fname, "w", encoding='utf-8') as f:
            json.dump(shard, f, ensure_ascii=False, indent=2)
        file_size = os.path.getsize(fname)
        print(f"{fname}: {len(shard)} объектов, {file_size/1024:.1f} КБ")
        results.append(file_size)
    return results

if __name__ == "__main__":
    all_objects = collect_json_objects(input_dirs)
    print(f"Всего объектов: {len(all_objects)}")

    shards, sizes = shard_by_size(all_objects, n_shards)
    print("Предварительная оценка размеров шардов (до serializing):", [s//1024 for s in sizes], "КБ")
    file_sizes = save_shards(shards, out_dir)
    print("Реальные размеры файлов (КБ):", [s//1024 for s in file_sizes])
    print("Готово!")
