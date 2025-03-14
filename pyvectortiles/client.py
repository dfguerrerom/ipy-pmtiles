import os
import json
import asyncio
from pathlib import Path
from typing import Dict, Union, Optional, Any, List
import shutil

import httpx
from pyvectortiles.logger import logger

from .server import TileServer
from .converter import TileConverter
from .utils import is_port_in_use, detect_file_type


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
        convert_if_needed: bool = True,
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
            convert_if_needed: Whether to convert the data source to PMTiles if needed.
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

        # Initialize the pmtiles_directory
        self.pmtiles_directory = self._determine_pmtiles_directory()

        # Process the data source
        if self.data_source:
            self._process_data_source(convert_if_needed)
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

    def _determine_pmtiles_directory(self) -> Path:
        """
        Determine the PMTiles directory based on input and data source.

        Args:
            pmtiles_directory: Explicitly provided directory or None

        Returns:
            Path object for the PMTiles directory
        """
        if self.data_source is not None:
            if self.data_source.suffix.lower() == ".pmtiles":
                return self.data_source.parent
            else:
                return self.data_source.parent / f"{self.data_source.stem}_pmtiles"
        return None

    def _find_pmtiles_files(self, directory: Path) -> List[Path]:
        """
        Find PMTiles files in a directory.

        Args:
            directory: Directory to search

        Returns:
            List of PMTiles file paths
        """
        return list(directory.glob("*.pmtiles"))

    def _ensure_directory(self, path: Path) -> Path:
        """
        Ensure a directory exists.

        Args:
            path: Directory path

        Returns:
            The same path
        """
        os.makedirs(path, exist_ok=True)
        return path

    def _process_data_source(self, convert_if_needed: bool) -> None:
        """
        Process the data source file.

        Args:
            convert_if_needed: Whether to convert non-PMTiles data sources
        """
        if self.data_source.suffix.lower() == ".pmtiles":
            self._handle_pmtiles()
        elif convert_if_needed:
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

    def _find_existing_pmtiles(self) -> None:
        """Find existing PMTiles files in the specified directory."""
        if self.pmtiles_directory and self.pmtiles_directory.exists():
            existing_pmtiles = self._find_pmtiles_files(self.pmtiles_directory)
            if existing_pmtiles:
                self.pmtiles_path = existing_pmtiles[0]
                logger.info(
                    f"Found PMTiles file in provided directory: {self.pmtiles_path}"
                )

    def _handle_pmtiles(self) -> None:
        """
        Handle a PMTiles file directly without conversion.

        If the metadata file is not present in the same folder as the PMTiles file,
        create it.
        """
        logger.info(f"Using PMTiles file directly: {self.data_source}")

        if not self.data_source.exists():
            raise FileNotFoundError(f"PMTiles file not found: {self.data_source}")

        # Ensure the pmtiles_directory exists
        self._ensure_directory(self.pmtiles_directory)

        # Copy the PMTiles file into the pmtiles_directory if necessary
        dest_file = self.pmtiles_directory / self.data_source.name
        if (
            not dest_file.exists()
            or dest_file.stat().st_mtime < self.data_source.stat().st_mtime
        ):
            logger.info(f"Copying PMTiles file to {dest_file}")
            shutil.copy2(self.data_source, dest_file)

        self.pmtiles_path = dest_file

        # Check if the metadata file exists in the same folder; create it if missing
        self._ensure_metadata_exists()

    def _ensure_metadata_exists(self) -> None:
        """
        Ensure metadata file exists for the PMTiles file.
        Creates it if missing.
        """
        metadata_path = self.pmtiles_directory / "metadata.json"
        if not metadata_path.exists():
            metadata = {
                "name": (
                    self.data_source.stem
                    if self.data_source
                    else self.pmtiles_path.stem
                ),
                "description": (
                    f"PMTiles from {self.data_source}"
                    if self.data_source
                    else "PMTiles file"
                ),
                "version": "1.0.0",
                "format": "pbf",
                "pmtiles_path": str(self.pmtiles_path),
                "attribution": "PMTiles source",
            }
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Created metadata file at {metadata_path}")
        else:
            logger.info(f"Metadata file already exists at {metadata_path}")

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

    def get_pmtiles_url(self) -> str:
        """
        Get the URL for the PMTiles file with its filePath.

        Returns:
            URL string for accessing the PMTiles file
        """
        return f"{self.server_url}/pmtiles?filePath={self.pmtiles_path}"

    async def _get_http_client(self) -> httpx.AsyncClient:
        """
        Get an HTTP client instance, creating one if needed.

        Returns:
            AsyncClient instance
        """
        if self._http_client is None:
            self._http_client = httpx.AsyncClient()
        return self._http_client

    async def get_metadata(self) -> Dict[str, Any]:
        """
        Fetch metadata about the tiles.

        Returns:
            Dictionary of metadata

        Raises:
            httpx.HTTPError: If the HTTP request fails
        """
        url = f"{self.server_url}/metadata?filePath={self.pmtiles_path}"
        client = await self._get_http_client()

        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Error fetching metadata: {e}")
            raise

    async def health_check(self) -> bool:
        """
        Check if the server is healthy.

        Returns:
            True if server is healthy, False otherwise
        """
        url = f"{self.server_url}/health"
        client = await self._get_http_client()

        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get("status") == "ok"
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def create_leaflet_layer(self, style: Optional[Dict[str, Any]] = None):
        """
        Create a PMTiles layer for ipyleaflet.

        Args:
            style: Optional custom style for the layer

        Returns:
            ipyleaflet.PMTilesLayer

        Raises:
            ImportError: If ipyleaflet is not installed
        """
        try:
            from ipyleaflet import PMTilesLayer

            default_style = {
                "layers": [
                    {
                        "id": "vector_layer",
                        "source": "pmtiles_source",
                        "source-layer": self.pmtiles_path.stem,
                        "type": "fill",
                        "paint": {
                            "fill-color": "red",
                            "border-color": "black",
                            "border-width": 1,
                        },
                    },
                ]
            }

            return PMTilesLayer(
                url=self.get_pmtiles_url(),
                style=style or default_style,
                attribution="Vector Tile Server",
                visible=True,
            )
        except ImportError:
            raise ImportError(
                "ipyleaflet is required to create a leaflet layer. "
                "Install it with 'pip install ipyleaflet'."
            )

    async def close(self) -> None:
        """
        Clean up resources used by the client.
        """
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def __aenter__(self) -> "TileClient":
        """
        Async context manager entry.

        Returns:
            Self
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Async context manager exit.
        """
        await self.close()
