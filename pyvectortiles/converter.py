"""
Converter component for the Vector Tile Server package.

This module provides functionality to convert vector data to PMTiles format.
"""

import os
import subprocess
import shutil
import logging
from pathlib import Path
from typing import Dict, Union, Optional, Any, List
import json
import tempfile

import geopandas as gpd

logger = logging.getLogger(__name__)


class TileConverter:
    """
    Converts vector data to PMTiles format using Tippecanoe or other methods.
    """

    def __init__(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path] = None,
        tippecanoe_path: str = "tippecanoe",
    ):
        """
        Initialize the tile converter.

        Args:
            input_path: Path to the input vector data
            output_path: Path to write the output tiles
            tippecanoe_path: Path to the tippecanoe executable
        """
        self.input_path = Path(input_path)
        self.output_path = (
            Path(output_path) if output_path else self.input_path.with_suffix(".tiles")
        )
        self.tippecanoe_path = tippecanoe_path

        # Validate input file existence
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

    def convert(
        self,
        method: str = "auto",
        max_zoom: int = 14,
        min_zoom: int = 0,
        **kwargs,
    ) -> Path:
        """
        Convert the input data to PMTiles.

        Args:
            method: Conversion method ('tippecanoe', 'python', or 'auto')
            max_zoom: Maximum zoom level
            min_zoom: Minimum zoom level
            **kwargs: Additional options for the converter

        Returns:
            Path: Path to the output PMTiles file
        """
        # Create output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)
        geojson_path = self._ensure_geojson(self.input_path)

        # Define output PMTiles file path
        pmtiles_path = self.output_path / f"{self.input_path.stem}.pmtiles"

        # Build Tippecanoe command for PMTiles output
        cmd = [
            self.tippecanoe_path,
            "-o",
            str(pmtiles_path),
            "-z",
            str(max_zoom),
            "-Z",
            str(min_zoom),
            "-l",
            self.input_path.stem,
            "--force",  # Overwrite existing files
        ]

        # Add additional options
        for key, value in kwargs.items():
            if len(key) == 1:
                cmd.append(f"-{key}")
            else:
                cmd.append(f"--{key.replace('_', '-')}")

            if value is not True:  # Only add value if it's not a boolean flag
                cmd.append(str(value))

        # Add input file
        cmd.append(str(geojson_path))

        logger.debug(f"Running command: {' '.join(cmd)}")

        # Run Tippecanoe
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.debug(f"Tippecanoe output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Tippecanoe error: {e.stderr}")
            raise RuntimeError(f"Tippecanoe conversion failed: {e.stderr}")

        # Create metadata file
        self._create_metadata(pmtiles_path)

        return self.output_path

    def _ensure_geojson(self, input_path: Path) -> Path:
        """
        Ensure the input file is in GeoJSON format, converting if necessary.

        Args:
            input_path: Path to the input file

        Returns:
            Path: Path to the GeoJSON file
        """
        # If already GeoJSON, return as is
        if input_path.suffix.lower() in (".geojson", ".json"):
            return input_path

        # Convert to GeoJSON
        logger.info(f"Converting {input_path} to GeoJSON")
        gdf = gpd.read_file(input_path)

        # Create a temporary GeoJSON file
        geojson_path = input_path.with_suffix(".geojson")
        gdf.to_file(geojson_path, driver="GeoJSON")

        return geojson_path

    def _create_metadata(self, pmtiles_path: Path):
        """
        Create a metadata file for the PMTiles.

        Args:
            pmtiles_path: Path to the PMTiles file
        """
        metadata = {
            "name": self.input_path.stem,
            "description": f"PMTiles generated from {self.input_path}",
            "version": "1.0.0",
            "format": "pbf",
            "source": str(self.input_path),
            "pmtiles_path": str(pmtiles_path),
            "attribution": "Generated with pyvectortiles",
        }

        metadata_path = self.output_path / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
