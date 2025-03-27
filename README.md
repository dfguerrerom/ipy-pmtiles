# 🌐 Ipy PM Tiles

A dynamic vector tile handler for visualizing pmtiles data in Jupyter, inspired by `localtileserver`, `leafmap`, `protomaps-leaflet` and `maplibre-gl`.

With `TileClient`, you can easily create a local vector tile server to visualize PMTiles in `ipyleaflet`.

If you have a vector file (`.shp`, `.geojson`, `.gpkg`, etc.), `TileClient` will convert it to PMTiles format using `tippecanoe`. If `tippecanoe` is not installed, an error will be raised. However, you can directly visualize local PMTiles as a data source.

## Installing Tippecanoe

[Tippecanoe](https://github.com/felt/tippecanoe) is a tool for generating vector tile sets from large collections of GeoJSON features. It is designed to make mapping large datasets easy and efficient.

```bash
git clone https://github.com/felt/tippecanoe.git
cd tippecanoe
make -j
make install
```
