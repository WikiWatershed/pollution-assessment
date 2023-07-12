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
import numpy as np
import geopandas as gpd
import geoviews as gv
import matplotlib.pyplot as plt
import matplotlib
import cartopy
import pyproj
from pollution_assessment.v2_plots.dynamic import DynamicPlotter
from pollution_assessment.v2_plots.static import StaticPlotter
from pollution_assessment.v2_plots.shared import (
    add_kwargs,
)
from typing import (
    get_args,
    Union,
    Protocol,
    Literal,
    Optional,
    TypedDict,
    Generator,
)

# set up geoviews
gv.extension('bokeh')
gv.renderer('bokeh').webgl = True

PlotTypes = Literal['static', 'dynamic']
OutputTypes = Union[gv.Overlay, plt.Figure]


class Plotter(Protocol):
    """Abstract base class for plotting functions."""
    @classmethod
    def make_plot(
        gdf: gpd.GeoDataFrame,
        **kwargs,
    ):
        """Returns a single plot."""
        raise NotImplementedError


PLOTTER_MAP: dict[PlotTypes, Plotter] = {
    'static': StaticPlotter,
    'dynamic': DynamicPlotter,
}


class ViewExtent(TypedDict):
    xmin: float
    ymin: float
    xmax: float
    ymax: float


class SplitOutInput(TypedDict):
    gdf: gpd.GeoDataFrame
    alpha_column: Optional[str]
    alpha_threshold: Optional[float]
    alpha_min_max: Optional[tuple[float, float]]


class LayerDict(TypedDict):
    """Dict to hold GeoDataFrame indices and info for each plot layer."""
    idxs: list[int | str]
    sub_gdf: gpd.GeoDataFrame
    geom_type: str
    alpha: float


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
    alpha_minmax: Optional[tuple[float, float]] = None,
) -> np.array:
    """Returns a list of alphas for a GeoDataFrame. Based on a threshold / column

    This will be used to grey out non-selected geographies.
    """
    if not alpha_minmax:
        alpha_minmax = (0.0, 1.0)

    alpha_min, alpha_max = alpha_minmax

    try:
        return np.where(gdf[alpha_col] < threshold, alpha_min, alpha_max)

    except KeyError:
        raise KeyError(
            f'param:alpha_col={alpha_col} not found in GeoDataFrame.'
        )


def split_out_layers(
    input_dict: SplitOutInput,
) -> Generator[LayerDict, SplitOutInput, None]:
    """Generator that splits a GeoDataFrame indices into layers.

    Used to plot multiple layers with different alpha values, 
    or different geometry types.
    """
    # get alphas
    apply_alpha = False
    if input_dict['alpha_column'] and input_dict['alpha_threshold']:
        apply_alpha = True
        if not input_dict['alpha_min_max']:
            input_dict['alpha_min_max'] = (0.0, 1.0)

        input_dict['gdf']['alpha_values'] = get_alphas(
            input_dict['gdf'],
            input_dict['alpha_column'],
            input_dict['alpha_threshold'],
            input_dict['alpha_min_max'],
        )

        unique_alphas = input_dict['gdf'].alpha_values.unique()

    # first check for different geometry types
    geom_types = input_dict['gdf'].geom_type.unique()
    for geom_type in geom_types:
        sub_gdf = input_dict['gdf'][input_dict['gdf'].geom_type == geom_type]

    # yield based on geometry type + alpha combo
        if not apply_alpha:
            yield LayerDict(
                idxs=sub_gdf.index.tolist(),
                sub_gdf=sub_gdf,
                geom_type=geom_type,
                alpha=1.0,
            )
        else:
            for alpha in unique_alphas:
                yield LayerDict(
                    idxs=sub_gdf[sub_gdf.alpha_values == alpha].index.tolist(),
                    sub_gdf=sub_gdf[sub_gdf.alpha_values == alpha].drop(
                        columns=['alpha_values'],
                    ),
                    geom_type=geom_type,
                    alpha=alpha,
                )


def make_map(
    gdf: gpd.GeoDataFrame,
    how: Optional[PlotTypes] = None,
    color_column: Optional[str] = None,
    cmap: Optional[str | matplotlib.colors.Colormap] = None,
    hover_columns: Optional[list[str]] = None,
    group_column: Optional[str] = None,
    group_subset: Optional[str | int | list[str | int]] = None,
    alpha_column: Optional[str] = None,
    alpha_threshold: Optional[float] = None,
    alpha_min_max: Optional[tuple[float, float]] = None,
    extent_buffer: Optional[float | int] = None,
    **kwargs,
) -> gv.Overlay | plt.Figure:
    """Top level plotting function.

    Arguments:
        gdf: GeoDataFrame with data to plot.
        how: Whether to return a static or dynamic plot.
            Default is dynamic w/ holoviews.
        color_column: Column to use for coloring.
        cmap: Colormap to use. Default is RdYlGn_r.
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

    # get plotter class
    plotter: Plotter = PLOTTER_MAP[how]

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

    # get color related arguments
    if not cmap:
        cmap = matplotlib.colormaps['RdYlGn_r']
    if isinstance(cmap, str):
        cmap = matplotlib.colormaps[cmap]
    elif not isinstance(cmap, matplotlib.colors.Colormap):
        raise TypeError(
            f'Invalid cmap={cmap}. Must be a matplotlib colormap obj or str.',
        )

    # get basic input dict
    args_dict = {
        'c': color_column,
        'cmap': cmap,
        'hover_cols': hover_columns,
        'xlim': (extent_dict['xmin'], extent_dict['xmax']),
        'ylim': (extent_dict['ymin'], extent_dict['ymax']),
    }

    # make a plot for each alpha grouping and/or geometry type
    output = None
    layer_gdfs = split_out_layers(SplitOutInput(
        gdf=gdf,
        alpha_column=alpha_column,
        alpha_threshold=alpha_threshold,
        alpha_min_max=alpha_min_max,
    ))
    for layer_dict in layer_gdfs:
        args_dict['alpha'] = layer_dict['alpha']
        plot = plotter.make_plot(
            layer_dict['sub_gdf'],
            **add_kwargs(args_dict, kwargs, warn=True),
        )

        if not output:
            output = plot
        else:
            output *= plot

    return output
