import math
import mapbox_vector_tile
import gzip
from pmtiles.reader import Reader, MmapSource

from shapely import GeometryCollection
from shapely.geometry import Point
from shapely.geometry import shape as shapely_shape
from shapely.affinity import scale as shapely_scale
from pyvectortiles.logger import logger


# TODO: remove this is for debugging purposes
class GeomCollector:
    def __init__(self):
        self.geom = []
        self.raw_geom = []
        self.query_point = None

    def collect(self, geom):
        self.geom.append(geom)

    def collect_raw(self, geom):
        self.raw_geom.append(geom)

    def display(self):
        geoms = []
        # conver to shapely geometries
        for geom in self.geom:
            geom = shapely_shape(geom)
            geoms.append(geom)

        return GeometryCollection(geoms)

    def display_raw(self):
        raw_geoms = []
        # conver to shapely geometries
        for raw in self.raw_geom:
            geom = shapely_shape(raw)
            raw_geoms.append(geom)

        return GeometryCollection(raw_geoms)


geom_collector = GeomCollector()


# Following methods aims to emulate a rendering engine to query features from a PMTiles file.
# It will more or less mimic what MapLibre GL JS does with queryRenderedFeatures.
# Apply style rules (filters, minzoom/maxzoom, layout conditions).
# Keep track of which features would actually be rendered.
# This requires the style file and evaluating feature properties against style.


def latlon_to_tile_coords(lat: float, lon: float, zoom: float) -> tuple:
    """Convert latitude and longitude to tile coordinates and pixel coordinates."""
    n = 2**zoom
    x = (lon + 180.0) / 360.0
    y = (
        1.0
        - math.log(math.tan(math.radians(lat)) + 1 / math.cos(math.radians(lat)))
        / math.pi
    ) / 2.0
    tile_x = int(x * n)
    tile_y = int(y * n)
    exact_x = x * n
    exact_y = y * n
    return tile_x, tile_y, exact_x, exact_y


def get_tile_data_with_overzoom(
    pmtiles_reader: Reader,
    requested_zoom: float,
    tile_x: int,
    tile_y: int,
):
    """Get tile data with overzooming support."""

    # TODO: I have to fix this to allow querying more than one level...

    requested_zoom = int(requested_zoom)
    logger.debug("    ####: Requested tile data", tile_x, tile_y, requested_zoom)
    tile_data = pmtiles_reader.get(requested_zoom, int(tile_x), int(tile_y))
    if tile_data:
        return tile_data, requested_zoom, tile_x, tile_y, 1
    else:
        return None, None, None, None, None


def evaluate_filter(filter_expr, properties):
    if not filter_expr:
        return True
    op = filter_expr[0]
    if op == "==":
        key, value = filter_expr[1], filter_expr[2]
        return properties.get(key) == value
    elif op == "!=":
        key, value = filter_expr[1], filter_expr[2]
        return properties.get(key) != value
    elif op == "in":
        key, *values = filter_expr[1:]
        return properties.get(key) in values
    elif op == "not in":
        key, *values = filter_expr[1:]
        return properties.get(key) not in values
    else:
        return False


def is_layer_visible_with_opacity(style_layer):
    layout = style_layer.get("layout", {})
    if layout.get("visibility", "visible") != "visible":
        return False

    paint = style_layer.get("paint", {})
    layer_type = style_layer.get("type")

    if layer_type == "fill":
        if float(paint.get("fill-opacity", 1)) == 0:
            logger.debug("Layer is not visible")
            return False
    elif layer_type == "line":
        if float(paint.get("line-opacity", 1)) == 0:
            return False
    elif layer_type == "symbol":
        icon_opacity = float(paint.get("icon-opacity", 1))
        text_opacity = float(paint.get("text-opacity", 1))
        if icon_opacity == 0 and text_opacity == 0:
            return False
    return True


def is_feature_rendered(feature, style_layer, zoom):
    """Determine if a feature is rendered based on style rules"""

    # TODO: IDK why but in the protomaps-leaflet version that is used, it renders
    # the features even if they're out of the zoom range. So I will bypass that rule
    # in the query_rendered_features method.

    # minzoom = style_layer.get("minzoom", 0)
    # maxzoom = style_layer.get("maxzoom", 24)
    # if not (minzoom <= zoom <= maxzoom):
    #     return False

    if not is_layer_visible_with_opacity(style_layer):
        return False

    filter_expr = style_layer.get("filter")
    if filter_expr:
        props = feature.get("properties", {})
        result = evaluate_filter(filter_expr, props)
        logger.debug(f"🔍 Filter expression: {filter_expr}")
        logger.debug(f"   Properties: {props}")
        logger.debug(f"   Filter result: {'✅' if result else '❌'}")
        if not result:
            return False

    return True


def transform_geometry_to_pixels(geom, tile_extent, tile_size, overzoom_scale=1):
    """Transform a geometry to pixel coordinates based on tile size and extent."""

    factor = (tile_size / tile_extent) * overzoom_scale
    geom = shapely_scale(geom, xfact=factor, yfact=factor, origin=(0, 0))

    # Mirroring the y-axis to match the pixel coordinate system
    mirrored_geom = shapely_scale(geom, xfact=1, yfact=-1, origin=(0, tile_size / 2))

    return mirrored_geom


def get_center_px(on_zoom_x, on_zoom_y, tile_x, tile_y, tile_size=256):
    """Convert tile coordinates to pixel coordinates."""
    return ((on_zoom_x - tile_x) * tile_size, (on_zoom_y - tile_y) * tile_size)


def decode_tile_data(tile_data):
    """Decode compressed (gzip) MVT tile data to extract features."""
    if tile_data is None:
        return None

    try:
        tile_data = gzip.decompress(tile_data)
    except OSError:
        pass

    tile = mapbox_vector_tile.decode(tile_data)
    return tile


def get_feature_unique_key(feature, seen_ids):
    """
    Returns a unique key for a feature.
    If the feature's 'id' is missing or has already been seen,
    we combine the id with a hash of the geometry (WKT) and the properties.
    """
    raw_id = feature.get("id")
    # If there's no id or it has been seen already, compute a fallback hash.
    if (raw_id is None) or (raw_id in seen_ids):
        # Compute a hash using the feature's geometry (WKT) and properties.
        geom_wkt = shapely_shape(feature["geometry"]).wkt
        fallback_hash = hash(
            (geom_wkt, frozenset(feature.get("properties", {}).items()))
        )
        return fallback_hash
    else:
        seen_ids.add(raw_id)
        return raw_id


def query_rendered_features(
    layer_data,
    style_layer,
    zoom,
    center_px,
    brush_size=1,
    tile_extent=4096,
    tile_size=256,
    overzoom_scale=1,
    geom_cache=None,
    distance_cache=None,
):
    """Query rendered features from a layer based on style rules and spatial filters."""

    if geom_cache is None:
        geom_cache = {}
    if distance_cache is None:
        distance_cache = {}

    results = []
    query_point = Point(center_px)
    layer_name = style_layer.get("id", "unknown_layer")
    # Key for the query center used in distance caching.
    center_key = (center_px[0], center_px[1])
    seen_ids = set()

    for feature in layer_data.get("features", []):
        # Skip if the feature isn't rendered at this zoom/style.
        geom_collector.collect_raw(feature["geometry"])
        if not is_feature_rendered(feature, style_layer, zoom):
            continue

        feature_unique_key = get_feature_unique_key(feature, seen_ids)

        # Create a cache key for geometry transformation.
        cache_key = (feature_unique_key, tile_extent, tile_size, overzoom_scale)
        if cache_key in geom_cache:
            transformed_geom = geom_cache[cache_key]
        else:
            geom = shapely_shape(feature["geometry"])
            transformed_geom = transform_geometry_to_pixels(
                geom, tile_extent, tile_size, overzoom_scale
            )
            geom_cache[cache_key] = transformed_geom

        # Create a key for the distance cache (depends on the query point too).
        dist_key = cache_key + center_key
        if dist_key in distance_cache:
            dist = distance_cache[dist_key]
        else:
            dist = transformed_geom.distance(query_point)
            distance_cache[dist_key] = dist

        # Apply type-specific spatial filtering.
        geom_type = transformed_geom.geom_type
        if geom_type == "Point":
            if dist < brush_size:
                results.append({"feature": feature, "layerName": layer_name})
        elif geom_type in ("LineString", "MultiLineString"):
            if dist < brush_size:
                results.append({"feature": feature, "layerName": layer_name})
        else:
            # For polygons (or similar), test if the point is inside.
            if query_point.within(transformed_geom):
                results.append({"feature": feature, "layerName": layer_name})

        geom_collector.collect(transformed_geom)

    return results


def query_rendered_features_from_pmtiles(
    pmtiles_path,
    style,
    lat,
    lon,
    desired_zoom,
    brush_size=1,
    tile_size=256,
    tile_extent=4096,
):

    features_by_id = {}

    # Shared caches across style layers.
    geom_cache = {}
    distance_cache = {}
    decoded_tile_cache = {}

    level_diff = 2

    geom_collector.geom = []
    geom_collector.raw_geom = []

    with open(pmtiles_path, "rb") as f:
        pmtiles_reader = Reader(MmapSource(f))
        for style_layer in style.get("layers", []):

            source_layer = style_layer.get("source-layer")
            if not source_layer:
                continue

            layer_maxzoom = style_layer.get("maxzoom", 14)
            data_zoom = min(desired_zoom - level_diff, layer_maxzoom)
            tile_x, tile_y, on_zoom_x, on_zoom_y = latlon_to_tile_coords(
                lat, lon, data_zoom
            )

            tile_data, used_zoom, used_tile_x, used_tile_y, scale_factor = (
                get_tile_data_with_overzoom(pmtiles_reader, data_zoom, tile_x, tile_y)
            )
            if tile_data is None:
                logger.debug("###: No tile data found for layer", source_layer)
                continue

            local_center_px = get_center_px(
                on_zoom_x, on_zoom_y, used_tile_x, used_tile_y, tile_size=256
            )
            local_center_px = (
                local_center_px[0] * scale_factor,
                local_center_px[1] * scale_factor,
            )
            logger.debug("local_center_px", local_center_px)

            geom_collector.query_point = Point(local_center_px)

            # Cache decoded tile data by source_layer and tile parameters.
            decoded_key = (
                source_layer,
                used_zoom,
                used_tile_x,
                used_tile_y,
                scale_factor,
            )
            if decoded_key in decoded_tile_cache:
                decoded_tile = decoded_tile_cache[decoded_key]
            else:
                decoded_tile = decode_tile_data(tile_data)
                decoded_tile_cache[decoded_key] = decoded_tile

            layer_data = decoded_tile.get(source_layer)
            if not layer_data:
                continue

            logger.debug("Length of layer data", len(layer_data.get("features", [])))

            rendered_features = query_rendered_features(
                layer_data,
                style_layer,
                used_zoom,
                local_center_px,
                brush_size,
                tile_extent,
                tile_size,
                scale_factor,
                geom_cache=geom_cache,
                distance_cache=distance_cache,
            )

            for item in rendered_features:
                # Generate a robust unique key for the feature.
                feature = item["feature"]
                fid = get_feature_unique_key(feature, set())
                if fid not in features_by_id:
                    features_by_id[fid] = item

    return list(features_by_id.values())
