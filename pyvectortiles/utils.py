"""
Utility functions for handling PMTiles.

This module provides helper functions for working with PMTiles format.
"""

import socket
from pathlib import Path
from typing import Union
import os


def get_free_port(start_port=8000, max_port=9000):
    """
    Find a free port on the local machine.

    Args:
        start_port: Port to start searching from
        max_port: Maximum port to search to

    Returns:
        int: A free port number
    """
    for port in range(start_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("localhost", port)) != 0:
                return port
    raise RuntimeError(
        f"Could not find a free port between {start_port} and {max_port}"
    )


def is_port_in_use(port):
    """
    Check if a port is in use.

    Args:
        port: Port to check

    Returns:
        bool: True if the port is in use, False otherwise
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0
