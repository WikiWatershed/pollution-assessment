"""
This file will contain updating plotting functions for the pollution assessment.
Author: @xaviernogueira

Goals:
* Composable plot outputs with simple inputs.
* Toggle dynamic vs static plots.
* Abstract COMID groupings.
* Lower level functions that can be wrapped into higher level functions.
"""
import geopandas as gpd
import holoviews as hv
import geoviews as gv
import matplotlib.pyplot as plt
from pathlib import Path
from typing import (
    Literal,
    Optional,
    TypedDict,
)

Choices = Literal['static', 'dynamic']


class ViewExtent(TypedDict):
    xmin: float
    ymin: float
    xmax: float
    ymax: float


def get_extent(
    gdf: gpd.GeoDataFrame,
    buffer: Optional[float | int] = None,
) -> ViewExtent:
    """
    Returns the extent of a GeoDataFrame with an optional buffer.

    """
    xmin, ymin, xmax, ymax = gdf.total_bounds
    return ViewExtent(
        xmin=xmin - buffer if buffer else xmin,
        ymin=ymin - buffer if buffer else ymin,
        xmax=xmax + buffer if buffer else xmax,
        ymax=ymax + buffer if buffer else ymax,
    )


def get_alphas(
    gdf: gpd.GeoDataFrame,
    alpha_col: str,
    threshold: float,
    alpha_min: Optional[float] = None,
    alpha_max: Optional[float] = None,
) -> list[float]:
    """Returns a list of alphas for a GeoDataFrame.

    This will be used to grey out non-selected geographies.
    """
    if not alpha_min:
        alpha_min = 0
    if not alpha_max:
        alpha_max = 1
    try:
        return [
            alpha_min if i < threshold else alpha_max for i in gdf[alpha_col]
        ]
    except KeyError:
        raise KeyError(
            f'Column {alpha_col} not found in GeoDataFrame.'
        )


def _static_plot(
    gdf: gpd.GeoDataFrame,
    color_column: str,
    **kwargs,
):
    """Returns a matplotlib static plot.

    NOTE: This should not be responsible for settings config.
        For example, sub-setting the GeoDataFrame should be done
        before passing it to this function.
    """
    pass


def _dynamic_plot(
    gdf: gpd.GeoDataFrame,
    color_column: str,
    **kwargs,
):
    """Returns a holoviews dynamic plot.

    NOTE: This should not be responsible for settings config.
    """
    pass


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


def make_plot(
    gdf: gpd.GeoDataFrame,
    how: Optional[Choices] = None,
    color_column: Optional[str] = None,
    group_column: Optional[str] = None,
    group_subset: Optional[str | int | list[str | int]] = None,
    alpha_column: Optional[str] = None,
    alpha_threshold: Optional[float] = None,
    alpha_min_max: Optional[tuple[float, float]] = None,
) -> object:
    """Top level plotting function.

    Arguments:
        gdf: GeoDataFrame with data to plot.
        how: Whether to return a static or dynamic plot.
            Default is dynamic w/ holoviews.
        color_column: Column to use for coloring.
        group_column: Column to use for grouping geometries.
            Ex: A HUC12 column could group COMIDs by HUC12.
        group_subset: Subset of group_column to plot.
            Ex: A list of a few HUC12 to plot, or 1 if a binary column is made.
        alpha_column: Column to use for alpha values.
        alpha_threshold: Threshold for alpha values.
            Above the value gets alpha_max, below gets alpha_min.
        alpha_min_max: Min and max values for alpha.
    """
    pass
