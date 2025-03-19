import requests
from pyvectortiles.client import TileClient

firenze_client = TileClient(
    "/home/dguerrero/1_modules/pyvectortiles/data/mbtiles/protomaps_firenze.pmtiles"
)
firenze_layer = firenze_client.create_leaflet_layer()

headers = {"range": "bytes=0-1023"}
response = requests.get(firenze_layer.url, headers=headers)

print("Status Code:", response.status_code)
print("Content-Range:", response.headers.get("Content-Range"))
print("Content-Length:", response.headers.get("Content-Length"))
print("Content-Length:", response.headers)
