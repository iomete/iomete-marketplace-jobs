## Using project in local/dev environment

```shell
python3 -m venv .env
source .env/bin/activate

pip install -e ."[dev]"
```

```bash
python app.py --format csv --output teams --teams-webhook <your-teams-webhook-url>
```

```bash
python app.py --output email  --format csv
```