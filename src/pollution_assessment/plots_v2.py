"""
This file will contain updating plotting functions for the pollution assessment.
Author: @xaviernogueira

Goals:
* Composable plot outputs with simple inputs.
* Toggle dynamic vs static plots.
* Abstract COMID groupings.
* Lower level functions that can be wrapped into higher level functions.
"""
import warnings
import geopandas as gpd
import hvplot.pandas
import holoviews as hv
import matplotlib.pyplot as plt
import pyproj
from typing import (
    get_args,
    Literal,
    Optional,
    TypedDict,
)

# set up geoviews
hv.extension('bokeh')
hv.renderer('bokeh').webgl = True

PlotTypes = Literal['static', 'dynamic']


class ViewExtent(TypedDict):
    xmin: float
    ymin: float
    xmax: float
    ymax: float


class CRS_Info(TypedDict):
    crs: pyproj.CRS
    x_label: str
    y_label: str


def get_crs_info(gdf) -> CRS_Info:
    """Returns CRS, and X/Y axis labels for a plot."""

    # NOTE: this assumes the first 2 as X, Y (true so far)
    crs = gdf.crs
    axis_info: list[pyproj._crs.Axis] = crs.axis_info
    return CRS_Info(
        crs=crs,
        x_label=axis_info[0].name,
        y_label=axis_info[1].name,
    )


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


def add_kwargs(
    kwargs: dict,
    other_kwarg: dict,
) -> dict:
    """Adds other_kwarg k, v pairs kwargs:dict, with warnings for conflicts."""
    for k, v in other_kwarg.items():
        if k in kwargs:
            warnings.warn(
                f'Kwarg conflict! Overriding default {k}={kwargs[k]} with {k}={v}.',
            )
        kwargs[k] = v
    return kwargs


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


def make_dynamic_plot(
    gdf: gpd.GeoDataFrame,
    **kwargs,
) -> hv.Overlay:
    """Returns a single holoviews dynamic plot.

    The same settings apply to all geometries passed in!
    To control settings for each geometry, use pyfunc:make_map().
    One can override either hvplot or custom defaults if desired via kwargs.
    This function passes all kwargs into hvplot. See options below:
        https://hvplot.holoviz.org/user_guide/Customization.html

    Required Arguments:
        gdf: GeoDataFrame with data to plot.

    Optional Arguments with non-intuitive custom defaults:
        hover_columns: list[str] - Columns to show in hover tooltip (default=[index.name]).
        logz: bool - Whether to use log scale for color (default=True).
        tiles: str - Basemap tile to use (default='CartoLight').

    Returns:
        hv.Overlay: Holoviews overlay of dynamic plots.
    """
    # get crs info
    crs_dict: CRS_Info = get_crs_info(gdf)

    # get relevant arguments
    color_col: str = kwargs.pop('c', None)
    hover_cols: list[str] = kwargs.pop('hover_columns', [])
    logz: bool = kwargs.pop('logz', True)
    tiles: str = kwargs.pop('tiles', 'CartoLight')
    crs: str = kwargs.pop('crs', crs_dict.crs.to_string())
    xlabel: str = kwargs.pop('xlabel', crs_dict.x_label)
    ylabel: str = kwargs.pop('ylabel', crs_dict.y_label)
    clabel: str = kwargs.pop('clabel', color_col)

    if crs != crs_dict.crs.to_string():
        warnings.warn(
            (
                f'Desired CRS={crs} does not match GeoDataFrame CRS {crs_dict.crs.to_string()}. '
                f'This may cause issues. Consider not providing a crs kwarg and using defaults.'
            ),
            type=UserWarning,
        )

    # return holoviews overlay
    return gdf.hvplot(
        geo=True,
        crs=crs,
        c=color_col,
        tiles=tiles,
        logz=logz,
        hover_cols=list(set([gdf.index.name] + hover_cols)),
        xlabel=xlabel,
        ylabel=ylabel,
        clabel=clabel,
        **kwargs,
    )


def make_map(
    gdf: gpd.GeoDataFrame,
    how: Optional[PlotTypes] = None,
    color_column: Optional[str] = None,
    hover_columns: Optional[list[str]] = None,
    group_column: Optional[str] = None,
    group_subset: Optional[str | int | list[str | int]] = None,
    alpha_column: Optional[str] = None,
    alpha_threshold: Optional[float] = None,
    alpha_min_max: Optional[tuple[float, float]] = None,
    extent_buffer: Optional[float | int] = None,
    **kwargs,
) -> hv.Overlay | plt.Figure:
    """Top level plotting function.

    Arguments:
        gdf: GeoDataFrame with data to plot.
        how: Whether to return a static or dynamic plot.
            Default is dynamic w/ holoviews.
        color_column: Column to use for coloring.
        group_column: Column to use for grouping geometries.
            Ex: A HUC12 column could group COMIDs by HUC12.
        group_subset: Subset of the index OR group_column to plot.
            Ex: A list of a few HUC12 to plot, or 1 if a binary column is made.
        alpha_column: Column to use for alpha values.
        alpha_threshold: Threshold for alpha values.
            Above the value gets alpha_max, below gets alpha_min.
        alpha_min_max: Min and max values for alpha.
    """
    # check plot type
    if not how:
        how: PlotTypes = 'dynamic'
    elif how not in get_args(PlotTypes):
        raise ValueError(
            f'Invalid how={how}. Must be one of {PlotTypes}.'
        )

    # get the column to group on
    if not group_column:
        group_column = 'index'

    # subset gdf if desired, and get extent
    if group_subset:
        if not isinstance(group_subset, list):
            group_subset = [group_subset]
        gdf = gdf.loc[getattr(gdf, group_column).isin(group_subset)]

    extent_dict: ViewExtent = get_extent(gdf, buffer=extent_buffer)

    # regroup gdf if desired
    if group_column != 'index':
        rows1 = gdf.shape[0]
        gdf = gdf.dropna(subset=[group_column])
        rows2 = gdf.shape[0]
        if rows1 != rows2:
            warnings.warn(
                f'Dropped {rows1 - rows2} rows due to missing {group_column} values.',
            )
        gdf = gdf.dissolve(by=group_column, observed=True)

    # hold layers based on alpha thresholds, as well as geometry type
    layer_gdfs: list[gpd.GeoDataFrame] = []
    if not alpha_column:
        layer_gdfs.append(gdf)

    # associates layer idxs with an alpha value
    alphas: dict[int, float] = {}

    # plot layers and return stacked output
    output = None
    for i, layer_gdf in enumerate(layer_gdfs):
        if how == 'dynamic':
            args_dict = {
                'c': color_column,
                'hover_columns': hover_columns,
                'alpha': alphas.get(i, None),
                'xlim': (extent_dict.xmin, extent_dict.xmax),
                'ylim': (extent_dict.ymin, extent_dict.ymax),
            }
            plot: hv.Overlay = make_dynamic_plot(
                layer_gdf,
                **add_kwargs(args_dict, kwargs),
            )

        else:
            raise NotImplementedError
            args_dict = {}
            plot: plt.Figure = make_static_plot(
                layer_gdf,
                **add_kwargs(args_dict, kwargs),
            )

        if not output:
            output = plot
        else:
            output *= plot

    return output


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
