import argparse
import csv
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True, type=Path)
    args = p.parse_args()
    source = args.source.expanduser().resolve()
    expected = json.loads((BASE / 'baseline/summary.json').read_text())
    print('проверка исходного CSV...', flush=True)
    digest = sha(source)
    if digest != expected['source_sha256']:
        raise ValueError('нужен тот же CSV, что в ДЗ № 3')
    folder = BASE / 'data'
    folder.mkdir(exist_ok=True)
    paths = [folder / f'part-{i}.csv' for i in range(4)]
    counts = [0] * 4
    streams = [path.open('w', encoding='utf-8', newline='') for path in paths]
    try:
        with source.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream)
            writers = [csv.DictWriter(s, fieldnames=reader.fieldnames) for s in streams]
            for writer in writers:
                writer.writeheader()
            for number, row in enumerate(reader):
                index = number % 4
                writers[index].writerow(row)
                counts[index] += 1
    finally:
        for stream in streams:
            stream.close()
    assert sum(counts) == expected['source_rows']
    manifest = {'source_sha256': digest, 'source_rows': sum(counts),
                'partitioning': 'CSV rows round-robin; headers retained; every row once',
                'parts': [{'key': f'input/{path.name}', 'rows': count,
                           'bytes': path.stat().st_size, 'sha256': sha(path)}
                          for path, count in zip(paths, counts)]}
    text = json.dumps(manifest, ensure_ascii=False, indent=2) + '\n'
    (folder / 'manifest.json').write_text(text)
    (BASE / 'evidence/input_manifest.json').write_text(text)
    (folder / 'source.json').write_text(json.dumps({'path': str(source)}))
    print('CSV совпадает с ДЗ № 3:', sum(counts), 'строк')
    print('четыре части:', counts)
    print('манифест: evidence/input_manifest.json')


if __name__ == '__main__':
    main()
