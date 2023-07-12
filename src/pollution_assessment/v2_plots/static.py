import geopandas as gpd
import matplotlib.pyplot as plt


class StaticPlot(plt.Figure):
    """Static plot wrapper class to enable hvplot style compositions.

    Maybe with axes not plt.Figure api?
    """
    def __init__(
        figure: plt.Figure,
    ):
        pass

    def __add__(
        self,
        other: plt.Figure,
    ):
        """Adds static plots side by side."""
        pass

    def __mul__(
        self,
        other: plt.Figure,
    ):
        """Adds static plots on top of each other."""
        pass


class StaticPlotter:
    """Uses matplotlib to make static plots."""

    @staticmethod
    def make_static_plot(
        gdf: gpd.GeoDataFrame,
        **kwargs,
    ):
        """Returns a matplotlib static plot.

        NOTE: This should not be responsible for settings config.
            For example, sub-setting the GeoDataFrame should be done
            before passing it to this function.
        """
        raise NotImplementedError
