import math
import mapbox_vector_tile
import gzip
from pmtiles.reader import Reader, MmapSource

from shapely.geometry import Point
from shapely.geometry import shape as shapely_shape
from shapely.affinity import scale as shapely_scale
from pyvectortiles.logger import logger


# TODO: remove this is for debugging purposes
class GeomCollector:
    def __init__(self):
        self.geom = []

    def collect(self, geom):
        self.geom = [geom]


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
    layer_maxzoom: int,
):
    """Get tile data with overzooming support."""

    # TODO: I have to fix this to allow querying more than one level...

    requested_zoom = int(requested_zoom)
    logger.debug("    ####: Requested tile data", tile_x, tile_y, requested_zoom)
    if requested_zoom > layer_maxzoom:
        logger.debug("###: Requested zoom is greater than layer maxzoom")
        scale_factor = 2 ** (requested_zoom - layer_maxzoom)
        fallback_tile_x = tile_x // scale_factor
        fallback_tile_y = tile_y // scale_factor
        tile_data = pmtiles_reader.get(layer_maxzoom, fallback_tile_x, fallback_tile_y)
        logger.debug(
            "              ####: Fallback tile data", fallback_tile_x, fallback_tile_y
        )
        if tile_data:
            logger.debug("              ####: Fallback tile data found")
            return (
                tile_data,
                layer_maxzoom,
                fallback_tile_x,
                fallback_tile_y,
                scale_factor,
            )
        else:
            return None, None, None, None, None
    else:
        logger.debug("###: Requested zoom is within layer maxzoom")
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

    logger.debug(f"Layer type: {layer_type}" f"\n   Paint properties: {paint}")

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

    logger.debug("Feature will be rendered")
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


def query_rendered_features(
    layer_data,
    style_layer,
    zoom,
    center_px,
    brush_size=1,
    tile_extent=4096,
    tile_size=256,
    overzoom_scale=1,
):
    """Query rendered features from a layer based on style rules and spatial filters."""

    unique_features = {}
    query_point = Point(center_px)
    logger.debug(f">>>> Querying point: {query_point}")

    for feature in layer_data.get("features", []):
        if not is_feature_rendered(feature, style_layer, zoom):
            continue

        if center_px and brush_size is not None:

            logger.debug(feature["geometry"])
            geom = shapely_shape(feature["geometry"])
            logger.debug(
                f"Transforming geom... with {tile_extent}, {tile_size} and overzoom {overzoom_scale}"
            )

            transformed_geom = transform_geometry_to_pixels(
                geom, tile_extent, tile_size, overzoom_scale
            )
            geom_collector.collect([transformed_geom, query_point])

            logger.debug("transformed_geom.bounds", transformed_geom.bounds)
            logger.debug("transformed_geom.coords", transformed_geom.__str__())

            distance = transformed_geom.distance(query_point)
            logger.debug("distance", distance)

            if transformed_geom.distance(query_point) > brush_size:
                continue

        # ID único del feature
        feature_id = feature.get("id")
        if feature_id is None:
            feature_id = hash(frozenset(feature.get("properties", {}).items()))

        if feature_id not in unique_features:
            unique_features[feature_id] = {
                "id": feature_id,
                "properties": feature.get("properties", {}),
            }

    return list(unique_features.values())


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

    tile_x, tile_y, on_zoom_x, on_zoom_y = latlon_to_tile_coords(lat, lon, desired_zoom)
    features_by_id = {}

    with open(pmtiles_path, "rb") as f:
        pmtiles_reader = Reader(MmapSource(f))

        for style_layer in style.get("layers", []):
            source_layer = style_layer.get("source-layer")
            if not source_layer:
                continue

            layer_maxzoom = style_layer.get("maxzoom", 24)

            tile_data, used_zoom, used_tile_x, used_tile_y, scale_factor = (
                get_tile_data_with_overzoom(
                    pmtiles_reader, desired_zoom, tile_x, tile_y, layer_maxzoom
                )
            )

            if tile_data is None:
                continue

            local_center_px = get_center_px(
                on_zoom_x, on_zoom_y, used_tile_x, used_tile_y, tile_size=256
            )
            local_center_px = (
                local_center_px[0] * scale_factor,
                local_center_px[1] * scale_factor,
            )

            logger.debug("local_center_px", local_center_px)

            if tile_data is None:
                continue

            decoded_tile = decode_tile_data(tile_data)
            layer_data = decoded_tile.get(source_layer)

            if not layer_data:
                continue

            rendered_features = query_rendered_features(
                layer_data,
                style_layer,
                used_zoom,
                local_center_px,
                brush_size,
                tile_extent,
                tile_size,
                scale_factor,
            )

            for feature in rendered_features:
                feature_id = feature["id"]
                if feature_id not in features_by_id:
                    features_by_id[feature_id] = feature

    return list(features_by_id.values())
