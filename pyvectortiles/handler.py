import json
from pathlib import Path
from typing import Any, Dict, Union

from aiopmtiles import Reader
import nest_asyncio

nest_asyncio.apply()  # This is needed to run the async code in the notebook


async def get_metadata(pmtiles_path: Union[str, Path]) -> Dict[str, Any]:
    async with Reader(str(pmtiles_path)) as src:
        return await src.metadata()


def get_source_bounds(
    metadata: dict, projection: str = "EPSG:4326", decimal_places: int = 6
):

    bounds = metadata["antimeridian_adjusted_bounds"]
    left, bottom, right, top = bounds.split(",")
    left, bottom, right, top = map(float, [left, bottom, right, top])

    return {
        "left": round(left, decimal_places),
        "bottom": round(bottom, decimal_places),
        "right": round(right, decimal_places),
        "top": round(top, decimal_places),
    }
