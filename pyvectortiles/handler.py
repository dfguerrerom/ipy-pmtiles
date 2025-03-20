from pathlib import Path
from typing import Any, Dict, Union
from pmtiles.reader import Reader, MmapSource


def get_metadata(pmtiles_path: Union[str, Path]) -> Dict[str, Any]:
    """Get metadata from a PMTiles file."""

    with open(pmtiles_path, "r") as f:
        reader = Reader(MmapSource(f))
        header = reader.header()
        metadata = reader.metadata()

    bounds = parse_bounds(header)

    return {
        **metadata,
        "bounds": bounds,
        "center": calculate_center(bounds),
    }


def parse_bounds(header, decimal_places: int = 7):
    left, bottom, right, top = (
        header["min_lon_e7"] / 1e7,
        header["min_lat_e7"] / 1e7,
        header["max_lon_e7"] / 1e7,
        header["max_lat_e7"] / 1e7,
    )

    return {
        "left": round(left, decimal_places),
        "bottom": round(bottom, decimal_places),
        "right": round(right, decimal_places),
        "top": round(top, decimal_places),
    }


def calculate_center(bounds):
    """Get center in the form of (y <lat>, x <lon>)"""

    extent = (bounds["bottom"], bounds["top"], bounds["left"], bounds["right"])

    return (
        (extent[1] - extent[0]) / 2 + extent[0],
        (extent[3] - extent[2]) / 2 + extent[2],
    )
