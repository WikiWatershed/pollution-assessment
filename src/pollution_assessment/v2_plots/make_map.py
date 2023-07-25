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


PlotTypes = Literal['static', 'dynamic']
OutputTypes = Union[gv.Overlay, plt.Figure]


class Plotter(Protocol):
    """Abstract base class for plotting functions."""
    @classmethod
    def make_plot(
        gdf: gpd.GeoDataFrame,
        first: bool,
        **kwargs,
    ) -> OutputTypes:
        """Returns a single plot.

        Arguments:
            gdf: GeoDataFrame with data to plot.
            first: Whether this is the first plot in a series of layers.

        Returns:
            The plot output for rendering in a notebook or layering.
        """
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


def get_cmap(
    cmap: Optional[str | matplotlib.colors.Colormap] = None,
) -> matplotlib.colors.Colormap:
    if not cmap:
        cmap = matplotlib.colormaps['RdYlGn_r']
    if isinstance(cmap, str):
        cmap = matplotlib.colormaps[cmap]
    elif not isinstance(cmap, matplotlib.colors.Colormap):
        raise TypeError(
            f'Invalid cmap={cmap}. Must be a matplotlib colormap obj or str.',
        )
    return cmap


def subset_columns(
    gdf: gpd.GeoDataFrame,
    hover_columns: Optional[list[str]] = None,
    color_column: Optional[str] = None,
    alpha_column: Optional[str] = None,
    group_column: Optional[str] = None,
) -> gpd.GeoDataFrame:
    """Subsets columns to reduce GeoDataFrame dimensionality."""
    if not hover_columns:
        hover_columns = []

    hover_columns = hover_columns + [
        'geometry',
        color_column,
        alpha_column,
        group_column,
    ]
    hover_columns = [c for c in hover_columns if c]

    # add the coloring, alpha, and group columns
    for col in hover_columns:
        if col not in gdf.columns:
            warnings.warn(
                f'Column {col} not found in GeoDataFrame.',
            )
            hover_columns.remove(col)

    return gdf[list(set(hover_columns))]


def add_alphas(
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

    # make sure correct min/max values are being used
    if alpha_min > alpha_max:
        raise ValueError(
            f'Invalid alpha_min_max={alpha_minmax}. '
            f'Min must be less than max.',
        )
    if alpha_min < 0.0 or alpha_max > 1.0:
        raise ValueError(
            f'Invalid alpha_min_max={alpha_minmax}. '
            f'Min and max must be between 0.0 and 1.0.',
        )

    try:
        gdf['alpha_value'] = np.where(
            gdf[alpha_col] < threshold,
            alpha_min,
            alpha_max,
        )
        return gdf

    except KeyError:
        raise KeyError(
            f'param:alpha_col={alpha_col} not found in GeoDataFrame.'
        )


def split_geometry_types(
    gdf: gpd.GeoDataFrame,
) -> Generator[LayerDict, gpd.GeoDataFrame, None]:
    """Generator that splits a GeoDataFrame indices into layers by geom_type."""

    # first check for different geometry types
    geom_types = gdf.geom_type.unique()
    for geom_type in geom_types:
        sub_gdf = gdf[gdf.geom_type == geom_type]

    # yield based on geometry type + alpha combo
        yield LayerDict(
            idxs=sub_gdf.index.tolist(),
            sub_gdf=sub_gdf,
            geom_type=geom_type,
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
            Default is (0.0, 1.0).
        extent_buffer: Buffer to add to extent (must be in same units as data).
        kwargs: Additional kwargs to pass to the plotter.

    Returns:
        The plot output for rendering in a notebook.
    """
    # check plot type
    if not how:
        how: PlotTypes = 'dynamic'
    elif how not in get_args(PlotTypes):
        raise ValueError(
            f'Invalid how={how}. Must be one of {PlotTypes}.'
        )

    # remove empty geometry rows
    if 'geometry' not in gdf.columns:
        raise KeyError(
            'GeoDataFrame must have a geometry column to plot!',
        )
    gdf = gdf.dropna(subset=['geometry'])

    # get plotter class
    plotter: Plotter = PLOTTER_MAP[how]

    # subset columns
    gdf: gpd.GeoDataFrame = subset_columns(
        gdf,
        hover_columns,
        color_column,
        alpha_column,
        group_column,
    )

    # un-convert any categorical columns
    for col in gdf.columns:
        if gdf[col].dtype.name == 'category':
            gdf[col] = gdf[col].astype(str)

    # get the column to group on
    if not group_column:
        group_column = 'index'

    # subset gdf if desired, and get extent
    if group_subset:
        if not isinstance(group_subset, list):
            group_subset = [group_subset]
        gdf = gdf.loc[getattr(gdf, group_column).isin(group_subset)].copy()

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
        gdf = gdf.dissolve(
            by=group_column,
            aggfunc='sum',
            sort=False,
            dropna=True,
            observed=True,
        )

    # get color related arguments
    cmap: matplotlib.colors.Colormap = get_cmap(cmap)

    # get alpha values as a column if desired
    if alpha_column and alpha_threshold:
        gdf = add_alphas(
            gdf,
            alpha_column,
            alpha_threshold,
            alpha_min_max,
        )

    # get basic input dict
    args_dict = {
        'c': color_column,
        'cmap': cmap,
        'xlim': (extent_dict['xmin'], extent_dict['xmax']),
        'ylim': (extent_dict['ymin'], extent_dict['ymax']),
    }

    # make a plot for each alpha grouping and/or geometry type
    output = None
    layer_gdfs = split_geometry_types(gdf)

    for layer_dict in layer_gdfs:
        plot = plotter.make_plot(
            layer_dict['sub_gdf'],
            first=bool(output is None),
            **add_kwargs(args_dict, kwargs, warn=True),
        )

        if not output:
            output = plot
        else:
            output *= plot

    return output
