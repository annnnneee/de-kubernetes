import hashlib
import json
from pathlib import Path

from common import BASE, read, save


def main():
    latest = read(BASE / 'results/latest.json')
    phases = ['setup', 'memory', 'storage', 'parallel', 'scheduling', 'quota']
    reports = {phase: read(BASE / latest[phase] / 'summary.json') for phase in phases}
    memory, parallel = reports['memory'], reports['parallel']
    pairs = BASE / latest['parallel'] / 'skills_2.jsonl'
    expected = BASE / 'baseline/skills_2.jsonl'
    top = sorted([json.loads(x) for x in pairs.read_text().splitlines()],
                 key=lambda x: (-x['frequency'], x['skills']))[:10]
    quota = reports['quota']
    checks = {
        'oom_killed_137': memory['oom']['reason'] == 'OOMKilled' and memory['oom']['exitCode'] == 137,
        'limit_above_measured_peak': memory['limit_mib'] * 1024**2 >= memory['profile']['cgroup_peak_bytes'] * 1.25,
        'tuned_job_matches_hw3': memory['tuned']['matches_hw3'],
        's3_objects_verified': len(reports['storage']['objects']) == 5 and all(o['verified'] for o in reports['storage']['objects']),
        'four_parts': sorted(m['index'] for m in parallel['maps']) == list(range(4)),
        'parallel_execution': parallel['overlap'],
        'full_pairs_match_hw3': pairs.read_bytes() == expected.read_bytes(),
        'top10_match_hw3': top == read(BASE / 'baseline/top10.json'),
        'insufficient_cpu': 'Insufficient cpu' in reports['scheduling']['pending']['message'],
        'started_after_cpu_release': reports['scheduling']['started_after_release'],
        'preemption': any(e['reason'] == 'Preempted' for e in reports['scheduling']['preemption_events']),
        'quota_rejects_creation': quota['rejected'] and not quota['second_pod_created'] and 'exceeded quota' in quota['error'],
        'logs_present': all((BASE / 'logs' / Path(latest[phase]).name / 'commands.txt').is_file() for phase in phases),
    }
    result = {'runs': {p: latest[p] for p in phases}, 'checks': checks,
              'pairs_sha256': hashlib.sha256(pairs.read_bytes()).hexdigest(), 'top10': top}
    save(BASE / 'evidence/verification.json', result)
    for name, ok in checks.items():
        print(name + ':', 'OK' if ok else 'FAIL')
    assert all(checks.values()), 'есть непройденные проверки'
    print('Все проверки пройдены. evidence/verification.json')


if __name__ == '__main__':
    main()
