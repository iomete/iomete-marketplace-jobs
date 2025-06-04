## Using project in local/dev environment

```shell
python3 -m venv .env
source .env/bin/activate

pip install -e ."[dev]"
```

```bash
python app.py --format csv --output teams --teams-webhook https://iomete0.webhook.office.com/webhookb2/14270a12-237d-41fe-9054-25c39a586b64@ad3da1d4-c1f5-4e94-9283-3524fa7bbd8c/IncomingWebhook/f506ea2acf784432869cad10f7c2c835/7451ca6f-cd39-471f-9944-3faaf7f3f71b/V2KmyAQ_1Xo3zqZ0SWbOEus6YRC5-kWD8ZwD7ZSpT0nI01
```

```bash
python app.py --output email  --format csv
```