# pipewatch

A lightweight CLI tool to monitor and alert on ETL pipeline health with pluggable backends.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git
cd pipewatch && pip install -e .
```

---

## Usage

Define your pipeline checks in a `pipewatch.yaml` config file:

```yaml
pipelines:
  - name: daily_sales_etl
    check: row_count
    threshold: 1000
    alert: slack
```

Then run the watcher:

```bash
pipewatch run --config pipewatch.yaml
```

Check the status of all monitored pipelines:

```bash
pipewatch status
```

Send a one-off alert manually:

```bash
pipewatch alert --pipeline daily_sales_etl --message "Manual trigger"
```

Pluggable backends for alerting (Slack, PagerDuty, email) and storage (PostgreSQL, SQLite, S3) can be configured in the same config file. See the [docs](docs/) for full backend configuration options.

---

## License

MIT © 2024 [yourname](https://github.com/yourname)