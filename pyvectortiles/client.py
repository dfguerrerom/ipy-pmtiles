import json
from pathlib import Path
from typing import Dict, Tuple, Union, Optional, Any, List
import shutil

import httpx
from pyvectortiles.handler import get_metadata
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
            host: Host where the server is running.
            port: Port where the server is running.
            converter: Custom TileConverter instance to use.
            conversion_options: Options to pass to the tile converter.
            allowed_directories: List of directories that can be accessed by the server.
            http_client: Custom HTTP client for testing.
        """
        logger.debug(f"Initializing tile client with data source: {data_source}")

        self.data_source = Path(data_source) if data_source else None
        self.host = host
        self.port = port
        self.converter = converter
        self.conversion_options = conversion_options or {}
        self.pmtiles_path = None
        self.allowed_directories = allowed_directories
        self._http_client = http_client

        self.pmtiles_directory = self.data_source.parent
        self.metadata = None

        self._process_data_source()

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

    @property
    def bounds(self) -> List:
        """Get the bounds of the PMTiles file."""
        return self.metadata.get("bounds", [])

    @property
    def center(self) -> Tuple:
        """Get the center of the PMTiles file."""
        return self.metadata.get("center", [])

    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata for the PMTiles file."""
        return get_metadata(self.pmtiles_path)

    def list_layers(self):
        """Return a list of available vector layer IDs from the metadata."""
        return [layer.get("id") for layer in self.metadata.get("vector_layers", [])]

    def _process_data_source(self) -> None:
        """Process the data source file."""

        if self.data_source.suffix.lower() == ".pmtiles":
            self._handle_pmtiles()

        elif existing_pmtiles := self._find_pmtiles_files(self.pmtiles_directory):
            if self.data_source.with_suffix(".pmtiles") in existing_pmtiles:
                self.pmtiles_path = self.data_source.with_suffix(".pmtiles")
                logger.debug(
                    f"Found PMTiles file in provided directory: {self.pmtiles_path}"
                )
        else:
            self._convert_vector_data()

        self.metadata = self.get_metadata()

    def _handle_pmtiles(self) -> None:
        """Handle a PMTiles file directly without conversion.

        If the metadata file is not present in the same folder as the PMTiles file,
        create it.
        """
        logger.debug(f"Using PMTiles file directly: {self.data_source}")

        if not self.data_source.exists():
            raise FileNotFoundError(f"PMTiles file not found: {self.data_source}")

        dest_file = self.pmtiles_directory / self.data_source.name
        if (
            not dest_file.exists()
            or dest_file.stat().st_mtime < self.data_source.stat().st_mtime
        ):
            logger.debug(f"Copying PMTiles file to {dest_file}")
            shutil.copy2(self.data_source, dest_file)

        self.pmtiles_path = dest_file

    def _convert_vector_data(self) -> None:
        """
        Process vector data formats and convert to PMTiles if needed.
        """
        logger.debug(
            f"Processing vector data: {self.data_source} -> {self.pmtiles_directory}"
        )

        # Use provided converter or create a new one
        converter = self.converter
        if not converter:
            converter = TileConverter(self.data_source, self.pmtiles_directory)

        # Convert the data
        if pmtiles_path := converter.convert(**self.conversion_options):
            logger.debug(f"Converted data to PMTiles: {self.pmtiles_path}")
            self.pmtiles_path = pmtiles_path

        else:
            raise RuntimeError(
                f"No PMTiles file was generated in {self.pmtiles_directory}"
            )

    def _ensure_server_running(self) -> None:
        """
        Ensure the tile server is running.
        """
        if self.port is not None and is_port_in_use(self.port):
            logger.debug(f"Using existing server at port {self.port}")
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
