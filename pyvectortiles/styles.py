import random
import colorsys


def generate_color_palette(palette_type="vibrant", num_colors=5):
    """
    Generate a color palette with different styles.

    Args:
        palette_type (str): Type of color palette
            Options: 'vibrant', 'pastel', 'earth', 'cool', 'warm'
        num_colors (int): Number of colors to generate

    Returns:
        list: List of hex color codes
    """
    palettes = {
        "vibrant": [
            "#FF6B6B",
            "#4ECDC4",
            "#45B7D1",
            "#FDCB6E",
            "#6C5CE7",  # Vibrant mix
            "#FF4500",
            "#1E90FF",
            "#32CD32",
            "#FF1493",
            "#FFD700",
        ],
        "pastel": [
            "#FFB3BA",
            "#BAFFC9",
            "#BAE1FF",
            "#FFFFBA",
            "#FFDFBA",
            "#E0BBE4",
            "#D4F0F0",
            "#DAEAF6",
            "#FFC6FF",
            "#F7EDE2",
        ],
        "earth": [
            "#8B4513",
            "#A0522D",
            "#D2691E",
            "#CD853F",
            "#DEB887",
            "#6B4423",
            "#5D4037",
            "#3E2723",
            "#795548",
            "#6D4C41",
        ],
        "cool": [
            "#0077BE",
            "#00A86B",
            "#4682B4",
            "#5F9EA0",
            "#48D1CC",
            "#20B2AA",
            "#008080",
            "#4169E1",
            "#1E90FF",
            "#6495ED",
        ],
        "warm": [
            "#FF4500",
            "#FF6347",
            "#FF7F50",
            "#FFD700",
            "#FFA500",
            "#FF8C00",
            "#FF4500",
            "#DC143C",
            "#B22222",
            "#FF1493",
        ],
    }

    colors = palettes.get(palette_type, palettes["cool"])

    # If not enough colors, generate additional colors
    while len(colors) < num_colors:
        # Generate a new random color with better color distribution
        h = random.random()
        s = 0.5 + random.random() * 0.5
        v = 0.5 + random.random() * 0.5

        r, g, b = [int(x * 255) for x in colorsys.hsv_to_rgb(h, s, v)]
        new_color = f"#{r:02x}{g:02x}{b:02x}"
        colors.append(new_color)

    return colors[:num_colors]


def random_color():
    """Generate a random hex color with better color distribution."""
    h = random.random()
    s = 0.5 + random.random() * 0.5
    v = 0.5 + random.random() * 0.5

    r, g, b = [int(x * 255) for x in colorsys.hsv_to_rgb(h, s, v)]
    return f"#{r:02x}{g:02x}{b:02x}"


def generate_default_map_style(
    metadata: dict,
    pmtiles_url: str,
    style_mode: str = "single_symbol",
    categorized_field: str = None,
    categorized_values: list = None,
    color_palette: str = "earth",
) -> dict:
    """
    Generate a default MapLibre/Mapbox style JSON based on vector layer metadata.

    Enhanced to support multiple color palettes and more flexible color generation.

    Parameters:
      metadata (dict): Dictionary containing metadata with a "vector_layers" key.
      pmtiles_url (str): URL or path to the PMTiles file.
      style_mode (str): "single_symbol" (default) for a uniform style, or "categorized" to
                        assign colors based on a specific field.
      categorized_field (str): The field name for categorization (required if style_mode is "categorized").
      categorized_values (list): List of distinct values for the field. Each value gets a random color.
      color_palette (str): Color palette to use. Options: 'vibrant', 'pastel', 'earth', 'cool', 'warm'

    Returns:
      dict: A style JSON dictionary.
    """
    layers = []
    default_palette = generate_color_palette(color_palette, 10)

    for i, vector_layer in enumerate(metadata.get("vector_layers", [])):
        layer_id = vector_layer.get("id")
        minzoom = vector_layer.get("minzoom", 0)
        maxzoom = vector_layer.get("maxzoom", 22)

        if style_mode == "single_symbol":
            fill_color = default_palette[i % len(default_palette)]
        elif style_mode == "categorized":
            if not categorized_field:
                raise ValueError(
                    "categorized_field must be provided when using categorized style_mode."
                )
            if categorized_values is None:
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

        fill_layer = create_fill_layer(layer_id, minzoom, maxzoom, fill_color)
        outline_layer = create_outline_layer(layer_id, minzoom, maxzoom)
        layers.extend([fill_layer, outline_layer])

    style = {
        "version": 8,
        "sources": {
            "pmtiles_source": {"type": "vector", "url": f"pmtiles://{pmtiles_url}"}
        },
        "layers": layers,
    }

    return style


def build_categorized_expression(categorized_field, categorized_values):
    expr = ["match", ["get", categorized_field]]
    for value in categorized_values:
        expr.extend([value, random_color()])
    expr.append("#CCCCCC")
    return expr


def create_fill_layer(layer_id, minzoom, maxzoom, fill_color):
    opacity = 0 if layer_id.lower() in ["mask", "earth"] else 0.5

    return {
        "id": f"{layer_id}-fill",
        "type": "fill",
        "source": "pmtiles_source",
        "source-layer": layer_id,
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "paint": {"fill-color": fill_color, "fill-opacity": opacity},
    }


def create_outline_layer(layer_id, minzoom, maxzoom):
    return {
        "id": f"{layer_id}-outline",
        "type": "line",
        "source": "pmtiles_source",
        "source-layer": layer_id,
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "paint": {"line-color": "#000000", "line-width": 1},
    }
