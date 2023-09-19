import geopandas as gpd
import matplotlib.pyplot as plt


class StaticPlotter:
    """Uses matplotlib to make static plots."""

    @staticmethod
    def make_plot(
        gdf: gpd.GeoDataFrame,
        **kwargs,
    ):
        """Returns a matplotlib static plot.

        NOTE: This should not be responsible for settings config.
            For example, sub-setting the GeoDataFrame should be done
            before passing it to this function.
        """
        raise NotImplementedError
