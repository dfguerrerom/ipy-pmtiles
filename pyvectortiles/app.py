from starlette.applications import Starlette
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from pyvectortiles.endpoints import (
    pmtiles_endpoint,
    metadata_endpoint,
    health_endpoint,
    logs_endpoint,
    shutdown_endpoint,
)


def create_app(tile_server_instance):
    """Create the Starlette application with routes and middleware."""

    async def shutdown_wrapper(request):
        return await shutdown_endpoint(request, tile_server_instance)

    async def metadata_wrapper(request):
        return await metadata_endpoint(request, tile_server_instance)

    async def pmtiles_wrapper(request):
        return await pmtiles_endpoint(request, tile_server_instance)

    routes = [
        Route("/health", health_endpoint),
        Route("/logs", logs_endpoint),
        Route("/shutdown", shutdown_wrapper),
        Route("/metadata", metadata_wrapper),
        Route("/pmtiles", pmtiles_wrapper),
    ]

    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=tile_server_instance.config.cors_origins,
            allow_methods=["GET"],
            allow_headers=["*"],
        )
    ]

    return Starlette(
        debug=tile_server_instance.config.debug, routes=routes, middleware=middleware
    )
