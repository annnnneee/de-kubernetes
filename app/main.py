import argparse
import csv
import hashlib
import io
import itertools
import json
import os
import re
import resource
import time
from collections import Counter
from pathlib import Path

import boto3
from botocore.config import Config

BASE = Path(__file__).resolve().parent
CONFIG = json.loads((BASE / 'config.json').read_text())
EXPECTED = json.loads((BASE / 'baseline/summary.json').read_text())
PATTERN = re.compile(CONFIG['title_regex'])


def s3():
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'],
                        aws_access_key_id=os.environ['S3_ACCESS_KEY'],
                        aws_secret_access_key=os.environ['S3_SECRET_KEY'],
                        region_name='us-east-1',
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))


class HashedReader(io.RawIOBase):
    def __init__(self, stream):
        self.stream, self.digest, self.size = stream, hashlib.sha256(), 0

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.stream.read(min(len(buffer), 65536))
        self.digest.update(data)
        self.size += len(data)
        buffer[:len(data)] = data
        return len(data)


def calculate(stream, chunk_rows):
    hashed = HashedReader(stream)
    reader = csv.DictReader(io.TextIOWrapper(io.BufferedReader(hashed), encoding='utf-8-sig', newline=''))
    counts, selected, total = Counter(), 0, 0
    print('начало расчёта, строк в порции:', chunk_rows, flush=True)
    while True:
        rows = list(itertools.islice(reader, chunk_rows))
        if not rows:
            break
        total += len(rows)
        for row in rows:
            if row['role'].strip() != CONFIG['role'] or not PATTERN.search(row['name']):
                continue
            skills = sorted({s.strip().lower() for s in row['key_skills'].split(';') if s.strip()})
            counts.update(itertools.combinations(skills, 2))
            selected += 1
        del rows
        print('обработано строк:', total, flush=True)
    return counts, selected, total, hashed.digest.hexdigest(), hashed.size


def encode_pairs(counts):
    return ''.join(json.dumps({'skills': pair, 'frequency': n}, ensure_ascii=False) + '\n'
                   for pair, n in sorted(counts.items())).encode()


def memory():
    cgroup = Path('/sys/fs/cgroup/memory.peak')
    return {'rss_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            'cgroup_peak_bytes': int(cgroup.read_text()) if cgroup.exists() else None}


def result(counts, selected, rows, started, complete):
    data = encode_pairs(counts)
    summary = {'variant': CONFIG['variant'], 'source_rows': rows, 'selected_rows': selected,
               'unique_pairs': len(counts), 'pair_occurrences': sum(counts.values()),
               'sha256': hashlib.sha256(data).hexdigest(),
               'top10': [{'skills': list(p), 'frequency': n}
                         for p, n in sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:10]],
               'elapsed_seconds': round(time.monotonic() - started, 3), **memory()}
    if complete:
        expected_hash = hashlib.sha256((BASE / 'baseline/skills_2.jsonl').read_bytes()).hexdigest()
        summary['matches_hw3'] = (summary['sha256'] == expected_hash
                                  and selected == EXPECTED['selected_rows']
                                  and rows == EXPECTED['source_rows'])
        if not summary['matches_hw3']:
            raise ValueError('результат отличается от ДЗ № 3')
    return data, summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['local', 'map', 'reduce', 'hold'])
    p.add_argument('--source', default='/input/vacancies.csv')
    p.add_argument('--chunk-rows', type=int, default=10000)
    p.add_argument('--seconds', type=int, default=300)
    args = p.parse_args()
    if args.chunk_rows < 1:
        p.error('chunk-rows должен быть положительным')
    started = time.monotonic()
    if args.mode == 'hold':
        print('задача запущена; удерживает заявку CPU, ожидание', args.seconds, 'с', flush=True)
        time.sleep(args.seconds)
        return
    if args.mode == 'local':
        with open(args.source, 'rb') as source:
            counts, selected, rows, source_hash, size = calculate(source, args.chunk_rows)
        if source_hash != EXPECTED['source_sha256'] or size != EXPECTED['source_bytes']:
            raise ValueError('исходный CSV отличается от ДЗ № 3')
        data, summary = result(counts, selected, rows, started, True)
        summary['source_sha256'] = source_hash
    else:
        client, bucket = s3(), os.environ.get('S3_BUCKET', 'hh-lake')
        prefix = os.environ['RUN_PREFIX']
        manifest = json.loads(client.get_object(Bucket=bucket, Key='input/manifest.json')['Body'].read())
        if manifest['source_sha256'] != EXPECTED['source_sha256']:
            raise ValueError('манифест относится к другому CSV')
        if args.mode == 'map':
            index = int(os.environ['JOB_COMPLETION_INDEX'])
            part = manifest['parts'][index]
            body = client.get_object(Bucket=bucket, Key=part['key'])['Body']
            try:
                counts, selected, rows, digest, size = calculate(body, args.chunk_rows)
            finally:
                body.close()
            if digest != part['sha256'] or rows != part['rows'] or size != part['bytes']:
                raise ValueError('часть CSV прочитана неверно')
            data, summary = result(counts, selected, rows, started, False)
            summary.update(index=index, input_key=part['key'], input_sha256=digest)
            stem = f'{prefix}/parts/{index}'
        else:
            counts, selected, rows = Counter(), 0, 0
            for index, part in enumerate(manifest['parts']):
                stem = f'{prefix}/parts/{index}'
                info = json.loads(client.get_object(Bucket=bucket, Key=stem + '.json')['Body'].read())
                data = client.get_object(Bucket=bucket, Key=stem + '.jsonl')['Body'].read()
                if info['index'] != index or info['input_sha256'] != part['sha256']:
                    raise ValueError('результат маппера относится к другой части')
                if hashlib.sha256(data).hexdigest() != info['sha256']:
                    raise ValueError('повреждён результат маппера')
                for line in data.splitlines():
                    row = json.loads(line)
                    counts[tuple(row['skills'])] += row['frequency']
                selected += info['selected_rows']
                rows += info['source_rows']
            data, summary = result(counts, selected, rows, started, True)
            stem = prefix + '/final'
        client.put_object(Bucket=bucket, Key=stem + '.jsonl', Body=data)
        client.put_object(Bucket=bucket, Key=stem + '.json',
                          Body=json.dumps(summary, ensure_ascii=False).encode())
    print('RESULT_JSON=' + json.dumps(summary, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
