import random


def random_color():
    """Generate a random hex color."""
    return "#%06x" % random.randint(0, 0xFFFFFF)


def build_categorized_expression(categorized_field, categorized_values):
    """
    Build a MapLibre GL "match" expression for categorizing features based on a field.

    Returns an expression that looks like:
      ["match", ["get", categorized_field], value1, color1, value2, color2, ..., default_color]
    """
    expr = ["match", ["get", categorized_field]]
    for value in categorized_values:
        expr.extend([value, random_color()])
    expr.append("#CCCCCC")  # default color if no value matches
    return expr


def create_fill_layer(layer_id, minzoom, maxzoom, fill_color):
    """
    Create a fill layer style for a vector layer.

    Parameters:
      layer_id (str): The id of the vector layer.
      minzoom (int): Minimum zoom level.
      maxzoom (int): Maximum zoom level.
      fill_color (str or list): The fill color or an expression defining it.

    Returns:
      dict: A style layer for polygon fills.
    """
    return {
        "id": f"{layer_id}-fill",
        "type": "fill",
        "source": "pmtiles_source",
        "source-layer": layer_id,
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "paint": {"fill-color": fill_color, "fill-opacity": 0.5},
    }


def create_outline_layer(layer_id, minzoom, maxzoom):
    """
    Create an outline (line) layer style for a vector layer.

    Parameters:
      layer_id (str): The id of the vector layer.
      minzoom (int): Minimum zoom level.
      maxzoom (int): Maximum zoom level.

    Returns:
      dict: A style layer for polygon outlines.
    """
    return {
        "id": f"{layer_id}-outline",
        "type": "line",
        "source": "pmtiles_source",
        "source-layer": layer_id,
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "paint": {"line-color": "#000000", "line-width": 1},
    }


def generate_default_map_style(
    metadata: dict,
    pmtiles_url: str,
    style_mode: str = "single_symbol",
    categorized_field: str = None,
    categorized_values: list = None,
) -> dict:
    """
    Generate a default MapLibre/Mapbox style JSON based on vector layer metadata.

    Parameters:
      metadata (dict): Dictionary containing metadata with a "vector_layers" key.
      pmtiles_url (str): URL or path to the PMTiles file.
      style_mode (str): "single_symbol" (default) for a uniform style, or "categorized" to
                        assign colors based on a specific field.
      categorized_field (str): The field name for categorization (required if style_mode is "categorized").
      categorized_values (list): List of distinct values for the field. Each value gets a random color.

    Returns:
      dict: A style JSON dictionary.
    """
    layers = []
    # Default palette for single_symbol mode
    default_palette = ["#FF6347", "#32CD32", "#1E90FF", "#FFD700", "#9370DB"]

    # Process each vector layer from metadata
    for i, vector_layer in enumerate(metadata.get("vector_layers", [])):
        layer_id = vector_layer.get("id")
        minzoom = vector_layer.get("minzoom", 0)
        maxzoom = vector_layer.get("maxzoom", 22)

        if style_mode == "single_symbol":
            # Pick a consistent color from the palette
            fill_color = default_palette[i % len(default_palette)]
        elif style_mode == "categorized":
            if not categorized_field:
                raise ValueError(
                    "categorized_field must be provided when using categorized style_mode."
                )
            if categorized_values is None:
                # In a complete solution, you could extract unique values from the PMTiles data.
                raise ValueError(
                    "categorized_values must be provided for categorized style_mode."
                )
            fill_color = build_categorized_expression(
                categorized_field, categorized_values
            )
        else:
            raise ValueError(
                "Invalid style_mode. Use 'single_symbol' or 'categorized'."
            )

        # Generate both fill and outline layers
        fill_layer = create_fill_layer(layer_id, minzoom, maxzoom, fill_color)
        outline_layer = create_outline_layer(layer_id, minzoom, maxzoom)
        layers.extend([fill_layer, outline_layer])

    # Build the complete style JSON
    style = {
        "version": 8,
        "sources": {
            "pmtiles_source": {"type": "vector", "url": f"pmtiles://{pmtiles_url}"}
        },
        "layers": layers,
    }

    return style
