import argparse
import hashlib
import json
import math
import secrets
import shutil
import time
from pathlib import Path

from common import (BASE, KUBE, NS, Transcript, apply_manifest, command, execute_job,
                    finish, get, job, job_pods, kube, read, save, snapshot, wait)

RUSTFS = 'rustfs/rustfs@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff'
NODE = 'kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a'


def setup(run):
    source = read(BASE / 'data/source.json')['path']
    kind = str(BASE / '.tools/kind') if (BASE / '.tools/kind').exists() else shutil.which('kind')
    if not kind:
        raise RuntimeError('установите kind: brew install kind')
    config = (BASE / 'manifests/kind.yaml').read_text().replace('__SOURCE_CSV__', json.dumps(source))
    (BASE / 'data/kind.yaml').write_text(config)
    clusters = command([kind, 'get', 'clusters'], quiet=True).stdout.splitlines()
    if 'de-kubernetes' not in clusters:
        command([kind, 'create', 'cluster', '--name', NS, '--image', NODE,
                 '--config', 'data/kind.yaml', '--kubeconfig', 'data/kubeconfig', '--wait', '180s'])
    elif not (BASE / 'data/kubeconfig').exists():
        command([kind, 'export', 'kubeconfig', '--name', NS, '--kubeconfig', 'data/kubeconfig'])
    kube('get', 'nodes', '-o', 'wide')
    kube('apply', '-f', 'manifests/namespace.yaml')
    command(['docker', 'build', '-t', 'de-skills:hw5', '.'])
    command([kind, 'load', 'docker-image', 'de-skills:hw5', '--name', NS])
    if command(['docker', 'image', 'inspect', RUSTFS], quiet=True, check=False).returncode:
        command(['docker', 'pull', RUSTFS])
    command(['docker', 'tag', RUSTFS, 'de-rustfs:hw5'])
    command([kind, 'load', 'docker-image', 'de-rustfs:hw5', '--name', NS])
    for name in ['de-skills:hw5', 'de-rustfs:hw5']:
        info = json.loads(command(['docker', 'image', 'inspect', name], quiet=True).stdout)
        save(run.output / (name.replace(':', '-') + '.json'),
             [{'Id': x['Id'], 'RepoDigests': x['RepoDigests'], 'Architecture': x['Architecture']} for x in info])
    kube('version', '-o', 'yaml')
    save(run.output / 'nodes.json', get('nodes'))
    finish(run, 'setup', {'cluster': NS, 'node_image': NODE, 'rustfs_source_image': RUSTFS})


def status(run):
    kube('get', 'nodes', '-o', 'wide')
    kube('get', 'pvc,svc,statefulset', '-n', NS)
    snapshot(run, 'status', selector='app=rustfs')
    finish(run, 'status', {'nodes': get('nodes'), 'namespace': NS})


def memory(run):
    suffix = run.name.split('-')[1] + '-' + run.name.split('-')[2]
    oom = job('job-oom.json', 'oom-' + suffix)
    pods, _ = execute_job(oom, run, expected='Failed')
    term = pods[0]['status']['containerStatuses'][0]['state']['terminated']
    assert term['reason'] == 'OOMKilled' and term['exitCode'] == 137, 'нужен OOMKilled'
    text = (run.logs / (pods[0]['metadata']['name'] + '.log')).read_text()
    assert 'начало расчёта' in text, 'OOM произошёл до начала расчёта'
    profile = job('job-profile.json', 'profile-' + suffix)
    _, summaries = execute_job(profile, run)
    measured = summaries[0]
    assert measured['matches_hw3']
    peak = measured['cgroup_peak_bytes']
    limit_mib = math.ceil(1.25 * peak / (16 * 1024**2)) * 16
    tuned = job('job-profile.json', 'tuned-' + suffix)
    resources = tuned['spec']['template']['spec']['containers'][0]['resources']
    resources['requests']['memory'] = str(math.ceil(peak / 1024**2)) + 'Mi'
    resources['limits']['memory'] = str(limit_mib) + 'Mi'
    save(BASE / 'manifests/job-tuned.json', tuned)
    print(f'пик cgroup: {peak / 1024**2:.2f} MiB; RSS: {measured["rss_peak_bytes"] / 1024**2:.2f} MiB')
    print(f'предел = ceil(пик × 1.25 / 16 MiB) × 16 MiB = {limit_mib} MiB')
    _, summaries = execute_job(tuned, run)
    summary = {'oom': term, 'profile': measured, 'limit_mib': limit_mib,
               'formula': 'ceil(cgroup_peak_bytes * 1.25 / (16 * 1024**2)) * 16 MiB',
               'tuned': summaries[0]}
    assert summary['tuned']['matches_hw3']
    save(BASE / 'evidence/memory.json', summary)
    print('OOMKilled подтверждён, с пределом расчёт завершён, совпадение с ДЗ № 3: True')
    finish(run, 'memory', summary)


def client():
    import boto3
    from botocore.config import Config
    keys = read(BASE / 'data/s3.json')
    return boto3.client('s3', endpoint_url='http://127.0.0.1:19100', region_name='us-east-1',
                        aws_access_key_id=keys['access-key'], aws_secret_access_key=keys['secret-key'],
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                                      connect_timeout=3, read_timeout=60, retries={'max_attempts': 2}))


def storage(run):
    keys_path = BASE / 'data/s3.json'
    if not keys_path.exists():
        save(keys_path, {'access-key': 'hw5-' + secrets.token_hex(8), 'secret-key': secrets.token_urlsafe(32)})
        keys_path.chmod(0o600)
    secret = {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 's3-credentials', 'namespace': NS},
              'type': 'Opaque', 'stringData': read(keys_path)}
    kube('apply', '-f', '-', input=json.dumps(secret), quiet=True)
    kube('apply', '-f', 'manifests/rustfs.yaml')
    kube('rollout', 'status', 'statefulset/rustfs', '-n', NS, '--timeout=300s')
    store = client()
    def ready():
        try:
            store.list_buckets()
            return True
        except Exception:
            return False
    wait(ready, 'RustFS не готов принимать S3-запросы', timeout=180)
    if 'hh-lake' not in [b['Name'] for b in store.list_buckets()['Buckets']]:
        store.create_bucket(Bucket='hh-lake')
    manifest = read(BASE / 'data/manifest.json')
    source = Path(read(BASE / 'data/source.json')['path'])
    store.upload_file(str(source), 'hh-lake', 'raw/it_vacancies_2024.csv')
    for part in manifest['parts']:
        store.upload_file(str(BASE / 'data' / Path(part['key']).name), 'hh-lake', part['key'])
        print(part['key'], 'строк:', part['rows'], flush=True)
    store.upload_file(str(BASE / 'data/manifest.json'), 'hh-lake', 'input/manifest.json')
    objects = []
    for key, expected in [('raw/it_vacancies_2024.csv', manifest['source_sha256'])] + [
            (p['key'], p['sha256']) for p in manifest['parts']]:
        body = store.get_object(Bucket='hh-lake', Key=key)['Body']
        digest = hashlib.sha256()
        size = 0
        try:
            while block := body.read(1024**2):
                digest.update(block)
                size += len(block)
        finally:
            body.close()
        assert digest.hexdigest() == expected, key
        objects.append({'key': key, 'bytes': size, 'sha256': digest.hexdigest(), 'verified': True})
    snapshot(run, 'storage')
    kube('get', 'pvc,svc,statefulset', '-n', NS)
    save(run.logs / 'storage-pvc.json', get('pvc'))
    print('S3: 4 узла RustFS и 4 PVC')
    finish(run, 'storage', {'objects': objects, 'inside_cluster': True, 'replicas': 4})


def parallel(run):
    measured = read(BASE / 'evidence/memory.json')
    suffix = run.name.split('-')[1] + '-' + run.name.split('-')[2]
    prefix = 'runs/' + run.name
    docs = [job('job-map.json', 'map-' + suffix), job('job-reduce.json', 'reduce-' + suffix)]
    for doc in docs:
        c = doc['spec']['template']['spec']['containers'][0]
        c['resources']['limits']['memory'] = str(measured['limit_mib']) + 'Mi'
        c['resources']['requests']['memory'] = str(math.ceil(measured['profile']['cgroup_peak_bytes'] / 1024**2)) + 'Mi'
        next(e for e in c['env'] if e['name'] == 'RUN_PREFIX')['value'] = prefix
    pods, maps = execute_job(docs[0], run)
    intervals = []
    for pod in pods:
        term = pod['status']['containerStatuses'][0]['state']['terminated']
        intervals.append({'pod': pod['metadata']['name'], 'start': term['startedAt'], 'end': term['finishedAt']})
    overlap = any(a['start'] < b['end'] and b['start'] < a['end']
                  for i, a in enumerate(intervals) for b in intervals[i + 1:])
    assert len(maps) == 4 and sorted(m['index'] for m in maps) == list(range(4))
    assert overlap, 'не подтверждено одновременное исполнение мапперов'
    _, reduced = execute_job(docs[1], run)
    summary = reduced[0]
    assert summary['matches_hw3'] and summary['top10'] == read(BASE / 'baseline/top10.json')
    store = client()
    store.download_file('hh-lake', prefix + '/final.jsonl', str(run.output / 'skills_2.jsonl'))
    assert (run.output / 'skills_2.jsonl').read_bytes() == (BASE / 'baseline/skills_2.jsonl').read_bytes()
    save(run.output / 'top10.json', summary['top10'])
    print('четыре части, parallelism=2, перекрытие времени мапперов:', overlap)
    for i, row in enumerate(summary['top10'], 1):
        print(i, ' + '.join(row['skills']), row['frequency'])
    print('топ-10 и полный файл частот совпадают с ДЗ № 3: True')
    finish(run, 'parallel', {'result': summary, 'maps': maps, 'intervals': intervals, 'overlap': overlap,
                              'top10_matches_hw3': True, 'full_file_matches_hw3': True, 's3_prefix': prefix})


def cpu(value):
    return int(value[:-1]) if value.endswith('m') else int(float(value) * 1000)


def hold(name, request, priority=None, seconds=300, namespace=NS):
    pod = {'restartPolicy': 'Never', 'terminationGracePeriodSeconds': 1,
           'containers': [{'name': 'hold', 'image': 'de-skills:hw5', 'imagePullPolicy': 'Never',
                           'args': ['hold', '--seconds', str(seconds)],
                           'resources': {'requests': {'cpu': str(request) + 'm', 'memory': '64Mi'},
                                         'limits': {'cpu': str(request) + 'm', 'memory': '128Mi'}}}]}
    if priority:
        pod['priorityClassName'] = priority
    return {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name, 'namespace': namespace},
            'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 360,
                     'template': {'metadata': {'labels': {'homework': 'kubernetes',
                                  'experiment': name.partition('-')[2]}}, 'spec': pod}}}


def running(name):
    return next((p for p in job_pods(name) if p['status'].get('phase') == 'Running'), None)


def scheduling(run):
    kube('apply', '-f', 'manifests/priorities.yaml')
    nodes = get('nodes')['items']
    assert len(nodes) == 1, 'опыт рассчитан на отдельный кластер kind с одним узлом'
    allocatable = cpu(nodes[0]['status']['allocatable']['cpu'])
    pods = json.loads(kube('get', 'pods', '-A', '-o', 'json', quiet=True).stdout)['items']
    requested = sum(cpu(c.get('resources', {}).get('requests', {}).get('cpu', '0'))
                    for p in pods if p['status'].get('phase') not in ['Succeeded', 'Failed']
                    and p['spec'].get('nodeName') == nodes[0]['metadata']['name']
                    for c in p['spec']['containers'])
    free = allocatable - requested
    request = math.ceil(free * 0.6 / 100) * 100
    assert 0 < request < free < 2 * request, 'недостаточно свободного CPU для опыта'
    print(f'CPU узла: {allocatable}m, занято заявками: {requested}m; свободно: {free}m, каждая задача: {request}m')
    suffix = run.name.split('-')[1] + '-' + run.name.split('-')[2]
    first, second = 'occupy-' + suffix, 'waiting-' + suffix
    apply_manifest(hold(first, request), run)
    wait(lambda: running(first), 'первая задача не запустилась')
    apply_manifest(hold(second, request, seconds=20), run)
    def pending():
        for pod in job_pods(second):
            for c in pod['status'].get('conditions', []):
                if c['type'] == 'PodScheduled' and c['status'] == 'False' and 'Insufficient cpu' in c.get('message', ''):
                    return c
    condition = wait(pending, 'не получен Pending с Insufficient cpu')
    print('ожидание:', condition['message'])
    snapshot(run, 'insufficient-cpu', selector='experiment=' + suffix)
    kube('delete', 'job', first, '-n', NS, '--wait=true')
    wait(lambda: running(second), 'задача не запустилась после освобождения CPU')
    snapshot(run, 'cpu-released', selector='experiment=' + suffix)
    kube('wait', '--for=condition=complete', 'job/' + second, '-n', NS, '--timeout=90s')
    low, high = 'low-' + suffix, 'high-' + suffix
    apply_manifest(hold(low, request, priority='de-low'), run)
    victim = wait(lambda: running(low), 'задача низкого приоритета не запустилась')
    snapshot(run, 'before-preemption', selector='experiment=' + suffix)
    apply_manifest(hold(high, request, priority='de-high', seconds=30), run)
    wait(lambda: running(high), 'задача высокого приоритета не запустилась')
    events = wait(lambda: [e for e in get('events')['items']
                           if e.get('involvedObject', {}).get('uid') == victim['metadata']['uid']
                           and e.get('reason') == 'Preempted'], 'нет события Preempted для низкоприоритетного Pod')
    snapshot(run, 'preemption', selector='experiment=' + suffix)
    print('вытеснен:', victim['metadata']['name'], 'приоритет 100; новая задача: приоритет 10000')
    print('событие:', events[0]['reason'], events[0].get('message', ''))
    kube('wait', '--for=condition=complete', 'job/' + high, '-n', NS, '--timeout=90s')
    kube('delete', 'job', low, '-n', NS, '--wait=true')
    finish(run, 'scheduling', {'allocatable_mcpu': allocatable, 'requested_before_mcpu': requested,
                               'request_per_job_mcpu': request, 'pending': condition,
                               'started_after_release': True, 'victim_uid': victim['metadata']['uid'],
                               'preemption_events': events})


def quota(run):
    namespace = 'de-kubernetes-quota'
    kube('apply', '-f', 'manifests/quota.yaml')
    kube('delete', 'pods', '-n', namespace, '-l', 'homework=kubernetes', '--ignore-not-found=true')
    suffix = run.name.split('-')[1] + '-' + run.name.split('-')[2]
    def pod(name):
        template = hold(name, 200)['spec']['template']
        return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': namespace,
                'labels': template['metadata']['labels']}, 'spec': template['spec']}
    first, second = pod('quota-first-' + suffix), pod('quota-second-' + suffix)
    apply_manifest(first, run)
    wait(lambda: get('resourcequota', 'compute', namespace).get('status', {}).get('used', {}).get('requests.cpu') == '200m',
         'квота не учла первую задачу')
    save(run.output / 'manifests' / (second['metadata']['name'] + '.json'), second)
    rejected = kube('create', '-f', '-', input=json.dumps(second), check=False)
    assert rejected.returncode != 0 and 'exceeded quota' in rejected.stderr and 'requests.cpu' in rejected.stderr
    assert not any(p['metadata']['name'] == second['metadata']['name'] for p in get('pods', namespace=namespace)['items'])
    state = get('resourcequota', 'compute', namespace)
    save(run.logs / 'resourcequota.json', state)
    kube('describe', 'resourcequota', 'compute', '-n', namespace)
    snapshot(run, 'quota', namespace)
    print('первая заявка: 200m, вторая: 200m, квота: 300m, вторая отклонена API, Pod не создан')
    kube('delete', 'pod', first['metadata']['name'], '-n', namespace, '--wait=true')
    finish(run, 'quota', {'rejected': True, 'error': rejected.stderr, 'quota': state['status'], 'second_pod_created': False})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['setup', 'status', 'memory', 'storage', 'parallel', 'scheduling', 'quota'])
    args = parser.parse_args()
    with Transcript(args.phase) as run:
        globals()[args.phase](run)


if __name__ == '__main__':
    main()
