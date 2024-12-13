import grpc
import re

from pyspark.sql.connect.client import ChannelBuilder


class CustomChannelBuilder(ChannelBuilder):
  def __init__(self, url: str, certificate: str = None):
    super().__init__(url)
    self.certificate = certificate

    hostname = re.search(r"sc://([a-zA-Z0-9.-]+):", url)

    if hostname:
      self.hostname = hostname.group(1)
    else:
      raise Exception("Hostname not defined")

  def toChannel(self) -> grpc.Channel:
    if ".crt" in self.certificate:
      with open(self.certificate, 'rb') as f:
        creds = grpc.ssl_channel_credentials(f.read())
    else:
      creds = grpc.ssl_channel_credentials(self.certificate.encode("utf-8"))

    return grpc.secure_channel(f"{self.hostname}:443", creds)
