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
import geoviews as gv
import matplotlib.pyplot as plt
import matplotlib
import cartopy
import pyproj
from typing import (
    get_args,
    Union,
    Protocol,
    Literal,
    Optional,
    TypedDict,
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


class ViewExtent(TypedDict):
    xmin: float
    ymin: float
    xmax: float
    ymax: float


class CRS_Info(TypedDict):
    crs: cartopy.crs.CRS
    x_label: str
    y_label: str


def get_crs_info(gdf) -> CRS_Info:
    """Returns CRS, and X/Y axis labels for a plot."""

    # NOTE: this assumes the first 2 as X, Y (true so far)
    crs: cartopy.crs.CRS = cartopy.crs.epsg(gdf.crs.to_epsg())
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
    warn: bool,
) -> dict:
    """Adds other_kwarg k, v pairs kwargs:dict, with warnings for conflicts."""
    for k, v in other_kwarg.items():
        if k in kwargs and warn:
            warnings.warn(
                f'Kwarg conflict! Overriding default {k}={kwargs[k]} with {k}={v}.',
            )
        kwargs[k] = v
    return kwargs


class StaticPlotter:
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


class DynamicPlotter:
    @staticmethod
    def _set_color_kwargs(
        gdf: gpd.GeoDataFrame,
        arg_dict: dict,
    ) -> tuple[gpd.GeoDataFrame, dict]:
        """Sets color kwargs for a GeoDataFrame.

        This allows constant color, or color by column.
        """
        # clean up kwargs
        if not arg_dict.get('hover_cols', None):
            arg_dict['hover_cols'] = []

        elif isinstance(arg_dict['hover_cols'], str):
            arg_dict['hover_cols'] = [arg_dict['hover_cols']]

        arg_dict['hover_cols'] = list(set(
            [gdf.index.name] + arg_dict['hover_cols']
        ))

        if not arg_dict['c']:
            if not arg_dict['one_color']:
                arg_dict['c'] = 'blue'
            arg_dict['clabel'] = None
            arg_dict['colorbar'] = False

            gdf['color'] = [arg_dict['c'] for i in range(len(gdf))]
            arg_dict['hover_cols'].insert(0, 'color')

        else:
            arg_dict['clabel'] = arg_dict['c']

        del arg_dict['one_color']
        arg_dict['legend'] = False

        return gdf, arg_dict

    @staticmethod
    def _draw_lines(
        gdf: gpd.GeoDataFrame,
        **kwargs,
    ) -> list[gv.Path]:
        """Returns a list of holoviews paths."""

        # get colormap normalization
        color_col: str = kwargs.pop('c', None)
        if not color_col in gdf.columns:
            color_hex = color_col
            use_cmap = False
        else:
            use_cmap = True
            cmap = kwargs.pop('cmap')
            vrange_dict: dict[str, float | int] = {
                'vmin': gdf[color_col].min(),
                'vmax': gdf[color_col].max(),
            }
            if kwargs['logz']:
                norm_func = matplotlib.colors.LogNorm(**vrange_dict)
            else:
                norm_func = matplotlib.colors.Normalize(**vrange_dict)

        # remove non-Path kwargs
        # TODO: fix this to add colorbar
        crs = kwargs.pop('crs')
        tiles = kwargs.pop('tiles')
        kwargs.pop('hover_cols', None)
        kwargs.pop('logz', None)
        kwargs.pop('legend', None)
        kwargs.pop('clabel', None)
        kwargs.pop('legend', None)
        kwargs.pop('colorbar', None)

        paths: list[gv.Path] = []
        for i, row in gdf.iterrows():
            if use_cmap:
                color_hex: tuple[float, float, float, float] = cmap(
                    norm_func(row[color_col])
                )
            paths.append(
                gv.Path(
                    row['geometry'],
                    crs=crs,
                ).opts(
                    color=color_hex,
                    **kwargs,
                )
            )
        output = None
        for path in paths:
            if not output:
                output = path
            else:
                output *= path
        return output * getattr(gv.tile_sources, tiles)

    @classmethod
    def make_plot(
        cls,
        gdf: gpd.GeoDataFrame,
        **kwargs,
    ) -> gv.Overlay:
        """Returns a single holoviews dynamic plot.

        The same settings apply to all geometries passed in!
        To control settings for each geometry, use pyfunc:make_map().
        One can override either hvplot or custom defaults if desired via kwargs.
        This function passes all kwargs into hvplot. See options below:
            https://hvplot.holoviz.org/user_guide/Customization.html

        Required Arguments:
            gdf: GeoDataFrame with data to plot. Note there must only be one geometry type!

        Optional Arguments with non-intuitive custom defaults:
            hover_cols: list[str] - Columns to show in hover tooltip (default=[index.name]).
            logz: bool - Whether to use log scale for color (default=True).
            tiles: str - Basemap tile to use (default='CartoLight').
            cmap: str - Colormap to use (default='RdYlGn_r').

        Returns:
            gv.Overlay: Holoviews overlay of dynamic plots.
        """
        # get crs info
        crs_dict: CRS_Info = get_crs_info(gdf)

        # set defaults
        arg_dict: dict = {
            'c': None,
            'one_color': None,
            'hover_cols': [],
            'logz': True,
            'tiles': 'CartoLight',
            'cmap': matplotlib.colormaps['RdYlGn_r'],
            'crs': crs_dict['crs'],
            'xlabel': crs_dict['x_label'],
            'ylabel': crs_dict['y_label'],
        }

        # add kwargs (overwriting defaults if desired)
        arg_dict = add_kwargs(
            arg_dict,
            kwargs,
            warn=False,
        )

        # clean up color kwargs
        gdf, arg_dict = cls._set_color_kwargs(
            gdf,
            arg_dict,
        )

        if arg_dict['crs'] != crs_dict['crs']:
            warnings.warn(
                (
                    f'Desired CRS={arg_dict["crs"]} does not match GeoDataFrame '
                    f'CRS {crs_dict.crs}. '
                    f'This may cause issues. Consider not providing a crs kwarg '
                    f'and using defaults.'
                ),
                type=UserWarning,
            )

        # remove empty geometry rows
        gdf = gdf.dropna(subset=['geometry'])

        # return plot based on geometry type
        if 'Line' not in gdf.geom_type.iloc[0]:
            return gdf.hvplot(
                geo=True,
                **arg_dict,
            )
        else:
            return cls._draw_lines(
                gdf,
                **arg_dict,
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

    # get plotter class
    plotter: Plotter = DynamicPlotter if how == 'dynamic' else StaticPlotter

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
    for i, layer_gdf in enumerate(layer_gdfs):
        args_dict['alpha'] = alphas.get(i, 1.0)
        plot = plotter.make_plot(
            layer_gdf,
            **add_kwargs(args_dict, kwargs, warn=True),
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
