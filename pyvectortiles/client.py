import os
import json
import asyncio
from pathlib import Path
from typing import Dict, Union, Optional, Any, List
import shutil

import httpx
from pyvectortiles.handler import (
    get_metadata,
    get_source_bounds,
)
from pyvectortiles.styles import generate_default_map_style
from pyvectortiles.logger import logger

from .server import TileServer
from .converter import TileConverter
from .utils import is_port_in_use


class TileClient:
    """
    A client for accessing PMTiles from a local server.

    This class triggers the lazy initialization of the server if needed.
    """

    def __init__(
        self,
        data_source: Union[str, Path] = None,
        host: str = "localhost",
        port: Optional[int] = None,
        converter: Optional[TileConverter] = None,
        conversion_options: Dict[str, Any] = None,
        allowed_directories: List[Union[str, Path]] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        """
        Initialize the tile client.

        Args:
            data_source: Path to the vector data source (GeoJSON, Shapefile, PMTiles, etc.)
            pmtiles_directory: Directory to store or find the PMTiles file and metadata.
            host: Host where the server is running.
            port: Port where the server is running.
            converter: Custom TileConverter instance to use.
            conversion_options: Options to pass to the tile converter.
            allowed_directories: List of directories that can be accessed by the server.
            http_client: Custom HTTP client for testing.
        """
        logger.info(f"Initializing tile client with data source: {data_source}")

        self.data_source = Path(data_source) if data_source else None
        self.host = host
        self.port = port
        self.converter = converter
        self.conversion_options = conversion_options or {}
        self.pmtiles_path = None
        self.allowed_directories = allowed_directories
        self._http_client = http_client

        self.pmtiles_directory = self._determine_pmtiles_directory()
        self.metadata = None

        if self.data_source:
            self._process_data_source()
        elif self.pmtiles_directory:
            self._find_existing_pmtiles()

        if self.pmtiles_path is None:
            raise ValueError(
                "PMTiles file is not available. Ensure that a valid data_source is provided or that the "
                "pmtiles_directory contains a PMTiles file."
            )

        # Ensure the server is running
        self._ensure_server_running()

        # Store the server URL
        self.server_url = f"http://{self.host}:{self.port}"

    @property
    def pmtiles_url(self) -> str:
        """Get the URL for the PMTiles file with its filePath."""
        return f"{self.server_url}/pmtiles?filePath={self.pmtiles_path}"

    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata for the PMTiles file."""
        return asyncio.run(get_metadata(self.pmtiles_path))

    def list_layers(self):
        """Return a list of available vector layer IDs from the metadata."""
        return [layer.get("id") for layer in self.metadata.get("vector_layers", [])]

    def bounds(
        self,
        projection: str = "EPSG:4326",
        return_polygon: bool = False,
        return_wkt: bool = False,
    ):
        bounds = get_source_bounds(self.metadata, projection=projection)
        extent = (bounds["bottom"], bounds["top"], bounds["left"], bounds["right"])
        if not return_polygon and not return_wkt:
            return extent
        # Safely import shapely
        try:
            from shapely.geometry import Polygon
        except ImportError as e:  # pragma: no cover
            raise ImportError(f"Please install `shapely`: {e}")
        coords = (
            (bounds["left"], bounds["top"]),
            (bounds["left"], bounds["top"]),
            (bounds["right"], bounds["top"]),
            (bounds["right"], bounds["bottom"]),
            (bounds["left"], bounds["bottom"]),
            (bounds["left"], bounds["top"]),  # Close the loop
        )
        poly = Polygon(coords)
        if return_wkt:
            return poly.wkt
        return poly

    def center(
        self,
        projection: str = "EPSG:4326",
        return_point: bool = False,
        return_wkt: bool = False,
    ):
        """Get center in the form of (y <lat>, x <lon>).

        Parameters
        ----------
        projection : str
            The srs or projection as a Proj4 string of the returned coordinates

        return_point : bool, optional
            If true, returns a shapely.Point object.

        return_wkt : bool, optional
            If true, returns a Well Known Text (WKT) string of center
            coordinates.

        """
        bounds = self.bounds(projection=projection)
        point = (
            (bounds[1] - bounds[0]) / 2 + bounds[0],
            (bounds[3] - bounds[2]) / 2 + bounds[2],
        )
        if return_point or return_wkt:
            # Safely import shapely
            try:
                from shapely.geometry import Point
            except ImportError as e:  # pragma: no cover
                raise ImportError(f"Please install `shapely`: {e}")

            point = Point(point)
            if return_wkt:
                return point.wkt

        return point

    def _determine_pmtiles_directory(self) -> Path:
        """
        Determine the PMTiles directory based on input and data source.

        Args:
            pmtiles_directory: Explicitly provided directory or None

        Returns:
            Path object for the PMTiles directory
        """
        if self.data_source:
            if self.data_source.suffix.lower() == ".pmtiles":
                return self.data_source.parent
            else:
                return self.data_source.parent / f"{self.data_source.stem}_pmtiles"
        return None

    def _process_data_source(self) -> None:
        """Process the data source file."""

        if self.data_source.suffix.lower() == ".pmtiles":
            self._handle_pmtiles()

        else:
            # For other vector formats
            if self.pmtiles_directory.exists():
                existing_pmtiles = self._find_pmtiles_files(self.pmtiles_directory)
                if existing_pmtiles:
                    self.pmtiles_path = existing_pmtiles[0]
                    logger.info(f"Using existing PMTiles file: {self.pmtiles_path}")
                else:
                    self._convert_vector_data()
            else:
                self._ensure_directory(self.pmtiles_directory)
                self._convert_vector_data()

        self.metadata = self.get_metadata()

    def _find_existing_pmtiles(self) -> None:
        """Find existing PMTiles files in the pmtiles directory."""

        if self.pmtiles_directory and self.pmtiles_directory.exists():
            existing_pmtiles = self._find_pmtiles_files(self.pmtiles_directory)
            if existing_pmtiles:
                self.pmtiles_path = existing_pmtiles[0]
                logger.info(
                    f"Found PMTiles file in provided directory: {self.pmtiles_path}"
                )

    def _handle_pmtiles(self) -> None:
        """Handle a PMTiles file directly without conversion.

        If the metadata file is not present in the same folder as the PMTiles file,
        create it.
        """
        logger.info(f"Using PMTiles file directly: {self.data_source}")

        if not self.data_source.exists():
            raise FileNotFoundError(f"PMTiles file not found: {self.data_source}")

        self._ensure_directory(self.pmtiles_directory)

        dest_file = self.pmtiles_directory / self.data_source.name
        if (
            not dest_file.exists()
            or dest_file.stat().st_mtime < self.data_source.stat().st_mtime
        ):
            logger.info(f"Copying PMTiles file to {dest_file}")
            shutil.copy2(self.data_source, dest_file)

        self.pmtiles_path = dest_file

    def _convert_vector_data(self) -> None:
        """
        Process vector data formats and convert to PMTiles if needed.
        """
        logger.info(
            f"Processing vector data: {self.data_source} -> {self.pmtiles_directory}"
        )

        # Use provided converter or create a new one
        converter = self.converter
        if not converter:
            converter = TileConverter(self.data_source, self.pmtiles_directory)

        # Convert the data
        converter.convert(**self.conversion_options)

        # Look for the generated PMTiles file
        pmtiles_files = self._find_pmtiles_files(self.pmtiles_directory)
        if pmtiles_files:
            self.pmtiles_path = pmtiles_files[0]
            logger.info(f"Created PMTiles file: {self.pmtiles_path}")
        else:
            raise RuntimeError(
                f"No PMTiles file was generated in {self.pmtiles_directory}"
            )

    def _ensure_server_running(self) -> None:
        """
        Ensure the tile server is running.
        """
        if self.port is not None and is_port_in_use(self.port):
            logger.info(f"Using existing server at port {self.port}")
            return

        server = TileServer.get_instance(
            host=self.host,
            port=self.port,
            allowed_directories=[self.pmtiles_directory]
            + (self.allowed_directories or []),
        )
        self.port = server.config.port

    def create_leaflet_layer(
        self,
        style: Optional[Dict[str, Any]] = None,
        layers_to_show: Optional[List[str]] = None,
    ) -> Any:
        """Create a PMTiles layer for ipyleaflet.

        Args:
            style: Optional custom style for the layer

        """
        try:
            from ipyleaflet import PMTilesLayer

            style_json = generate_default_map_style(self.metadata, self.pmtiles_url)

            logger.debug(f"Generated style JSON: {json.dumps(style_json, indent=2)}")

            if layers_to_show:
                if not all(layer in self.list_layers() for layer in layers_to_show):
                    raise ValueError(
                        f"Invalid layer IDs provided. Available layers: {self.list_layers()}"
                    )
                style_json["layers"] = [
                    layer
                    for layer in style_json["layers"]
                    if layer["source-layer"] in layers_to_show
                ]

                logger.debug(f"Filtered style JSON: {json.dumps(style_json, indent=2)}")

            return PMTilesLayer(
                url=self.pmtiles_url,
                style=style or style_json,
                attribution="Vector Tile Server",
                visible=True,
            )
        except ImportError:
            raise ImportError(
                "ipyleaflet is required to create a leaflet layer. "
                "Install it with 'pip install ipyleaflet'."
            )

    @staticmethod
    def _find_pmtiles_files(directory: Path) -> List[Path]:
        """Find PMTiles files in a directory."""

        return list(directory.glob("*.pmtiles"))

    @staticmethod
    def _ensure_directory(path: Path) -> Path:
        """Ensure a directory exists."""
        path.mkdir(parents=True, exist_ok=True)

        return path
