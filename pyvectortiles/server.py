import os
import threading
import time
from pathlib import Path
from typing import Union, Optional, List

import uvicorn

from pyvectortiles.app import create_app


from .utils import get_free_port, is_port_in_use
from pyvectortiles.logger import logger


class ServerConfig:
    """Configuration for the tile server."""

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        cors_origins: List[str] = None,
        allowed_directories: List[Union[str, Path]] = None,
        debug: bool = True,
    ):
        self.host = host
        self.port = port
        self.cors_origins = cors_origins or ["*"]
        self.allowed_directories = [
            Path(d).resolve() for d in (allowed_directories or [Path.cwd()])
        ]
        self.debug = debug


class TileServer:
    """
    A singleton server for serving PMTiles.

    This class implements a lazy initialization pattern and ensures only
    one server instance is running per process. The server is agnostic: it
    does not require a pre-configured PMTiles file. Instead, it serves the
    file requested by the client via the /pmtiles/{filename} endpoint.
    """

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, **config) -> "TileServer":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(**config)
            else:
                # If allowed_directories is provided, merge it into the current configuration.
                if "allowed_directories" in config and config["allowed_directories"]:
                    new_dirs = [
                        Path(d).resolve() for d in config["allowed_directories"]
                    ]
                    for d in new_dirs:
                        if d not in cls._instance.config.allowed_directories:
                            cls._instance.config.allowed_directories.append(d)

            if not cls._instance.is_running:
                cls._instance.start()

            return cls._instance

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        auto_start: bool = False,
        allowed_directories: List[Union[str, Path]] = None,
        cors_origins: List[str] = None,
        debug: bool = True,
    ):
        """
        Initialize the tile server.

        Args:
            host: Host to bind the server to.
            port: Port to bind the server to (if None, a free port will be found).
            auto_start: Whether to start the server immediately.
            allowed_directories: List of directories that files can be served from.
            cors_origins: List of allowed CORS origins.
            debug: Whether to run the server in debug mode.
        """
        self.config = ServerConfig(
            host=host,
            port=port if port is not None else get_free_port(),
            cors_origins=cors_origins,
            allowed_directories=allowed_directories,
            debug=debug,
        )

        self.is_running = False
        self.server_thread = None
        self.shutdown_event = threading.Event()
        self.app = create_app(self)

        if auto_start:
            self.start()

    def start(self) -> None:
        """Start the server in a background thread."""
        if self.is_running:
            logger.info(
                f"Server already running at http://{self.config.host}:{self.config.port}"
            )
            return

        if is_port_in_use(self.config.port):
            logger.warning(
                f"Port {self.config.port} is already in use. Finding a new port..."
            )
            self.config.port = get_free_port()

        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()

        # Reset shutdown event
        self.shutdown_event.clear()

        if not self._wait_for_server():
            raise TimeoutError(f"Server failed to start within timeout period")

        self.is_running = True
        logger.info(
            f"PMTiles server running at http://{self.config.host}:{self.config.port}"
        )

    def _run_server(self) -> None:
        """Run the uvicorn server."""
        config = uvicorn.Config(
            self.app,
            host=self.config.host,
            port=self.config.port,
            log_level="debug" if self.config.debug else "info",
        )
        server = uvicorn.Server(config)

        # Run the server until shutdown is requested
        def run():
            server.run()

        server_thread = threading.Thread(target=run)
        server_thread.start()

        # Wait for shutdown event
        self.shutdown_event.wait()

        # Stop the server
        server.should_exit = True
        server_thread.join()

    def _wait_for_server(self, timeout: int = 5, interval: float = 0.1) -> bool:
        """
        Wait for the server to start.

        Args:
            timeout: Maximum time to wait in seconds
            interval: Sleep interval between checks

        Returns:
            True if server started successfully, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if is_port_in_use(self.config.port):
                return True
            time.sleep(interval)
        logger.warning(f"Server didn't start within {timeout} seconds")
        return False

    def stop(self) -> None:
        """Stop the server properly."""
        if self.is_running:
            logger.info("Stopping server...")
            self.shutdown_event.set()

            # Give the server some time to shut down
            if self.server_thread and self.server_thread.is_alive():
                self.server_thread.join(timeout=5)

            self.is_running = False
            logger.info("Server stopped")

    def __del__(self) -> None:
        """Ensure server resources are cleaned up."""
        self.stop()
