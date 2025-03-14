"""
Utility functions for handling PMTiles.

This module provides helper functions for working with PMTiles format.
"""

import socket
from pathlib import Path
from typing import Union, Optional, Tuple
import mimetypes
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
            if s.connect_ex(('localhost', port)) != 0:
                return port
    raise RuntimeError(f"Could not find a free port between {start_port} and {max_port}")

def is_port_in_use(port):
    """
    Check if a port is in use.

    Args:
        port: Port to check

    Returns:
        bool: True if the port is in use, False otherwise
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def detect_file_type(file_path: Union[str, Path]) -> str:
    """
    Detect the file type based on extension and content.

    Args:
        file_path: Path to the file

    Returns:
        str: Detected file type ('pmtiles', 'geojson', 'shapefile', etc.)
    """
    path = Path(file_path)
    
    # Check by extension
    ext = path.suffix.lower()
    if ext == '.pmtiles':
        return 'pmtiles'
    elif ext in ('.geojson', '.json'):
        return 'geojson'
    elif ext == '.shp':
        return 'shapefile'
    elif ext == '.gpkg':
        return 'geopackage'
    
    # For additional checks based on file content, add more logic here
    
    # Default fallback
    return 'unknown'

def is_pmtiles_file(file_path: Union[str, Path]) -> bool:
    """
    Check if a file is a valid PMTiles file.

    Args:
        file_path: Path to the file to check

    Returns:
        bool: True if the file is a valid PMTiles file, False otherwise
    """
    # Simple check based on extension
    if not str(file_path).lower().endswith('.pmtiles'):
        return False
    
    # Check if file exists
    if not os.path.exists(file_path):
        return False
    
    # Basic header check (could be enhanced with actual PMTiles format validation)
    try:
        with open(file_path, 'rb') as f:
            # Read first few bytes to check signature
            header = f.read(6)
            # PMTiles v3 signature is "PMTiles" in ASCII
            # This is a simplified check - actual implementation would be more robust
            return header.startswith(b'PMTile')
    except Exception:
        return False