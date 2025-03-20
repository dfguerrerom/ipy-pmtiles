from typing import Iterator, Optional, TYPE_CHECKING

import re
from pathlib import Path

from starlette.responses import (
    Response,
    PlainTextResponse,
    FileResponse,
    StreamingResponse,
)
from starlette.requests import Request

from pyvectortiles.logger import logger

if TYPE_CHECKING:
    from pyvectortiles.server import TileServer


def _file_iterator(
    path: Path, start: int, length: int, chunk_size: int = 8192
) -> Iterator[bytes]:
    """
    Stream file content in chunks.

    Args:
        path: Path to the file
        start: Starting byte position
        length: Number of bytes to read
        chunk_size: Size of chunks to read

    Yields:
        Chunks of file data
    """
    with open(path, "rb") as f:
        f.seek(start)
        bytes_read = 0
        while bytes_read < length:
            chunk = f.read(min(chunk_size, length - bytes_read))
            if not chunk:
                break
            bytes_read += len(chunk)
            yield chunk


def _validate_file_path(
    file_path: Path, tile_server_instance: "TileServer"
) -> Optional[str]:
    """
    Validate that a file path is within allowed directories.

    Args:
        file_path: Path to validate

    Returns:
        Error message if validation fails, None if valid
    """

    try:
        resolved_path = file_path.resolve()
        for allowed_dir in tile_server_instance.config.allowed_directories:
            try:
                resolved_path.relative_to(allowed_dir)
                return None
            except ValueError:
                continue

        return "Access denied: File path is outside allowed directories"
    except Exception as e:
        return f"Invalid file path: {str(e)}"


async def pmtiles_endpoint(
    request: Request, tile_server_instance: "TileServer"
) -> Response:
    """
    Serve a byte-range of the requested PMTiles file.
    The client must specify the filename in the URL.
    """
    logger.debug("Serving PMTiles file from endpoint")
    file_path_str = request.query_params.get("filePath")
    if not file_path_str:
        return PlainTextResponse("Filename not specified", status_code=400)

    file_path = Path(file_path_str)
    validation_error = _validate_file_path(file_path, tile_server_instance)
    if validation_error:
        return PlainTextResponse(validation_error, status_code=403)

    if not file_path.exists():
        return PlainTextResponse("PMTiles file not found", status_code=404)

    file_size = file_path.stat().st_size
    range_header = request.headers.get("range")

    if range_header is None:
        return FileResponse(
            path=str(file_path),
            media_type="application/octet-stream",
            filename=str(file_path.name),
        )

    range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
    if not range_match:
        return PlainTextResponse("Invalid Range header", status_code=400)

    start = int(range_match.group(1))
    end_str = range_match.group(2)
    end = int(end_str) if end_str else file_size - 1

    logger.debug(f"Range request: {start}-{end}")

    if start >= file_size:
        return PlainTextResponse("Requested Range Not Satisfiable", status_code=416)
    if end >= file_size:
        end = file_size - 1

    length = end - start + 1

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
    }

    # Use streaming response for better memory management
    return StreamingResponse(
        _file_iterator(file_path, start, length),
        status_code=206,
        headers=headers,
        media_type="application/octet-stream",
    )


async def shutdown_endpoint(
    request: Request, tile_server_instance: "TileServer"
) -> Response:
    """Endpoint to trigger server shutdown."""

    tile_server_instance.shutdown_event.set()
    return PlainTextResponse("Server shutdown initiated", media_type="text/plain")
