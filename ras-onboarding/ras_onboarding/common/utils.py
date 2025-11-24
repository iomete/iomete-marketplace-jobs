from typing import Dict

import requests


def make_request(url: str, method: str,headers: Dict[str, str], params: Dict[str, str], data: Dict[str, str]):
    resp = requests.request(method, url, headers=headers, params=params, data=data)
    resp.raise_for_status()
    return resp.json()