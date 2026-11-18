# Kubernetes

Бондаренко Анна Андреевна

**Вариант № 2: «Программист, разработчик на Python».**

[Отчёт PDF](report/main.pdf)

Расчёт пар навыков из ДЗ № 3 в Kubernetes: ограничение памяти,
параллельные задания с S3, ожидание CPU, вытеснение и квота.
Эталон: 3 892 вакансии, 17 798 пар, 81 302 вхождение.

Проверены `OOMKilled`, успешный повтор с пределом 1088 MiB при измеренном
пике 857,84 MiB, совпадение полного файла частот, `Insufficient cpu`,
вытеснение `Preempted` и отклонение запроса по квоте.

## Файлы

- `Dockerfile`, `app/` — образ и расчёт пар навыков.
- `manifests/` — кластер kind, задания, RustFS, приоритеты и квота.
- `scripts/` — подготовка данных и запуск опытов.
- `baseline/` — частоты и топ-10 из ДЗ № 3.
- `logs/` — вывод команд, логи и состояния Kubernetes.
- `results/`, `evidence/` — измерения, результаты и проверки.

## Запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/prepare.py --source ~/Downloads/it_vacancies_2024.csv
python scripts/lab.py setup
python scripts/lab.py status
```

```bash
python scripts/lab.py memory
python scripts/lab.py storage
python scripts/lab.py parallel
python scripts/lab.py scheduling
python scripts/lab.py quota
python scripts/verify.py
```

Подробности опытов и выводы приведены в [отчёте](report/main.pdf).
