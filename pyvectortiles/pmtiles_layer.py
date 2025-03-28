from ipyleaflet import PMTilesLayer

from pyvectortiles.feature_query import query_rendered_features_from_pmtiles


class LeafletPMTilesLayer(PMTilesLayer):

    @property
    def pmtiles_path(self):
        return self.url.split("filePath=")[1]

    def get_data_from_coords(self, lat, lon, zoom):
        """Get features at a specific latitude, longitude, and zoom level."""

        data = query_rendered_features_from_pmtiles(
            self.pmtiles_path, self.style, lat, lon, zoom
        )

        # remove the geometry from the features
        for element in data:
            if "geometry" in element["feature"]:
                del element["feature"]["geometry"]

        return data
