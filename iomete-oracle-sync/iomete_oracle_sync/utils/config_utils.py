import os
import json


def get_config():
  application_conf = os.getenv("APPLICATION_CONF")

  if os.getenv("ENV") == "local":
    with open('app_conf.json', 'r') as file:
      return json.load(file)

  if application_conf:
    return json.loads(application_conf)

  with open('/etc/configs/app_conf.json', 'r') as file:
    return json.load(file)
