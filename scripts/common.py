import datetime
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
NS = 'de-kubernetes'
KUBE = ['kubectl', '--kubeconfig', 'data/kubeconfig', '--context', 'kind-de-kubernetes']


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def command(args, check=True, quiet=False, input=None):
    if not quiet:
        print('$', shlex.join(map(str, args)), flush=True)
    proc = subprocess.run(list(map(str, args)), cwd=BASE, text=True, input=input,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=900)
    if not quiet or (check and proc.returncode):
        print(proc.stdout + proc.stderr, end='', flush=True)
    if check and proc.returncode:
        raise RuntimeError(f'команда завершилась с кодом {proc.returncode}: {args[0]}')
    return proc


def kube(*args, **kwargs):
    return command(KUBE + list(args), **kwargs)


def get(kind, name=None, namespace=NS):
    return json.loads(kube('get', kind, *([name] if name else []), '-n', namespace,
                           '-o', 'json', quiet=True).stdout)


def wait(test, message, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = test()
        if value:
            return value
        time.sleep(2)
    raise TimeoutError(message)


class Transcript:
    def __init__(self, name):
        self.name = datetime.datetime.now().astimezone().strftime('%Y%m%d-%H%M%S-%f-') + name
        self.logs = BASE / 'logs' / self.name
        self.output = BASE / 'results' / self.name
        self.logs.mkdir(parents=True)
        self.output.mkdir(parents=True)

    def __enter__(self):
        self.original = sys.stdout
        self.file = (self.logs / 'commands.txt').open('w')
        sys.stdout = self
        print('время:', datetime.datetime.now().astimezone().isoformat())
        print('запуск:', self.name)
        return self

    def write(self, text):
        self.original.write(text)
        self.file.write(text)

    def flush(self):
        self.original.flush()
        self.file.flush()

    def __exit__(self, kind, error, traceback):
        if error:
            print('ОШИБКА:', type(error).__name__, str(error))
        print('журнал:', self.logs.relative_to(BASE))
        sys.stdout = self.original
        self.file.close()


def apply_manifest(doc, run):
    save(run.output / 'manifests' / (doc['metadata']['name'] + '.json'), doc)
    return kube('apply', '-f', '-', input=json.dumps(doc))


def snapshot(run, label, namespace=NS, selector=None):
    for resource in ['pods', 'jobs', 'events']:
        save(run.logs / f'{label}-{resource}.json', get(resource, namespace=namespace))
    output = kube('get', 'pods', '-n', namespace, '-o', 'wide',
                  *(['-l', selector] if selector else [])).stdout
    (run.logs / f'{label}-pods.txt').write_text(output)


def job(template, name):
    doc = read(BASE / 'manifests' / template)
    doc['metadata']['name'] = name
    doc['spec']['template'].setdefault('metadata', {})['labels'] = {'homework': 'kubernetes'}
    return doc


def job_pods(name):
    return [p for p in get('pods')['items'] if p['metadata'].get('labels', {}).get('job-name') == name]


def execute_job(doc, run, expected='Succeeded'):
    name = doc['metadata']['name']
    apply_manifest(doc, run)
    def done():
        pods = job_pods(name)
        if any(p['status'].get('phase') == 'Failed' for p in pods):
            return pods
        completed = sum(p['status'].get('phase') == 'Succeeded' for p in pods)
        return pods if completed >= doc['spec'].get('completions', 1) else None
    pods = wait(done, f'задание {name} не завершилось', timeout=660)
    summaries = []
    for pod in pods:
        pod_name = pod['metadata']['name']
        text = kube('logs', pod_name, '-n', NS, quiet=True, check=False).stdout
        (run.logs / (pod_name + '.log')).write_text(text)
        describe = kube('describe', 'pod', pod_name, '-n', NS, quiet=True).stdout
        (run.logs / (pod_name + '.txt')).write_text(describe)
        for line in text.splitlines():
            if line.startswith('RESULT_JSON='):
                summaries.append(json.loads(line.split('=', 1)[1]))
        states = [c.get('state', {}).get('terminated', {})
                  for c in pod['status'].get('containerStatuses', [])]
        print(name, pod['status'].get('phase'),
              [(s.get('reason'), s.get('exitCode')) for s in states], flush=True)
    snapshot(run, name, selector='job-name=' + name)
    assert all(p['status'].get('phase') == expected for p in pods), 'неожиданный исход задания'
    return pods, summaries


def finish(run, phase, summary):
    save(run.output / 'summary.json', summary)
    path = BASE / 'results/latest.json'
    latest = read(path) if path.exists() else {}
    latest[phase] = str(run.output.relative_to(BASE))
    save(path, latest)
    print('результаты:', run.output.relative_to(BASE))
