import warnings
import geopandas as gpd
import matplotlib
import hvplot.pandas
import numpy as np
import concurrent.futures
import geoviews as gv
import holoviews as hv
from pandas import Series
from pollution_assessment.v2_plots.shared import (
    CRS_Info,
    get_crs_info,
    add_kwargs,
)
from typing import (
    Generator,
    TypedDict,
    Optional,
)

# set up geoviews
gv.extension('bokeh')
gv.renderer('bokeh').webgl = True


class DynamicLineInput(TypedDict):
    """Input for a generator required to maintain formatting for lines"""
    gdf: gpd.GeoDataFrame
    args: dict
    apply_alpha: bool


class DynamicLineOutput(TypedDict):
    gdf: gpd.GeoDataFrame
    alpha: float
    color: tuple[float, float, float, float]


def _make_colorbar(
    cmap: matplotlib.colors.Colormap,
    clim: tuple[float, float],
    clabel: str,
    logz: bool,
) -> gv.Overlay:
    """Makes a standalone colorbar for our dynamic line plots"""
    if logz:
        minv, maxv = np.log10(clim[0]), np.log10(clim[1])
        clabel = f'log10({clabel})'
    else:
        minv, maxv = clim

    return hv.HeatMap(
        [(0, 0, minv), (0, 1, maxv)]
    ).opts(
        cmap=cmap,
        colorbar=True,
        alpha=0,
        clabel=clabel,
        logz=logz,
        show_frame=False,
    )


def _set_hover_args(
    gdf: gpd.GeoDataFrame,
    arg_dict: dict,
) -> dict:
    # get hover columns argument formatted
    if not arg_dict.get('hover_cols', None):
        arg_dict['hover_cols'] = [
            c for c in gdf.columns if c != 'geometry'
        ]

    elif isinstance(arg_dict['hover_cols'], str):
        arg_dict['hover_cols'] = [arg_dict['hover_cols']]

    if gdf.index.name:
        arg_dict['hover_cols'].insert(0, gdf.index.name)

    arg_dict['hover_cols'] = list(set(arg_dict['hover_cols']))

    return arg_dict


def _set_color_args(
    gdf: gpd.GeoDataFrame,
    arg_dict: dict,
) -> tuple[gpd.GeoDataFrame, dict]:
    """Sets color kwargs for a GeoDataFrame.

    This allows constant color, or color by column.
    """
    # set static color if desired
    if not arg_dict['c']:
        if not arg_dict['one_color']:
            arg_dict['c'] = 'blue'
        arg_dict['clabel'] = None
        del arg_dict['cmap']
        arg_dict['colorbar'] = False

        gdf['color'] = [arg_dict['c'] for i in gdf.index]

    # set dynamic color if desired
    else:
        arg_dict['clabel'] = arg_dict['c']
        arg_dict['colorbar'] = True

    del arg_dict['one_color']
    arg_dict['legend'] = False

    return gdf, arg_dict


def _generate_lines(
    input_dict: DynamicLineInput,
) -> Generator[DynamicLineInput, DynamicLineOutput, None]:
    """Yields a a row of a GeoDataFrame with a color and alpha value."""

    if input_dict['apply_alpha']:
        alphas: Series = input_dict['gdf'].pop('alpha_value')
    else:
        alphas: Series = Series(
            [1.0 for i in range(len(input_dict['gdf']))],
            index=input_dict['gdf'].index,
        )

    # format colors
    color_col = input_dict['args'].pop('c', None)
    cmap = input_dict['args'].pop('cmap', None)

    if 'colors' not in input_dict['gdf'].columns:
        vrange_dict: dict[str, float | int] = {
            'vmin': input_dict['args'].pop('vmin'),
            'vmax': input_dict['args'].pop('vmax'),
        }

        if input_dict['args']['logz']:
            norm_func = matplotlib.colors.LogNorm(**vrange_dict)
        else:
            norm_func = matplotlib.colors.Normalize(**vrange_dict)

        input_dict['gdf']['colors'] = (
            input_dict['gdf'][color_col]
            .apply(norm_func)
            .apply(cmap)
        )
    colors = input_dict['gdf'].pop('colors')

    # generate lines
    for idx, row in input_dict['gdf'].iterrows():
        row_gdf = gpd.GeoDataFrame(
            [row],
            geometry='geometry',
            crs=input_dict['args']['crs'],
        )
        yield DynamicLineOutput(
            gdf=row_gdf,
            alpha=alphas.loc[idx],
            color=colors[idx],
        )


def _make_path(
    line_output: DynamicLineOutput,
    args_dict: dict,
) -> gv.Path:
    """Returns a holoviews path."""
    args_dict_copy = args_dict.copy()
    return gv.Path(
        line_output['gdf'],
        crs=args_dict_copy.pop('crs'),
    ).opts(
        color=line_output['color'],
        alpha=line_output['alpha'],
        tools=['hover'],
        **args_dict_copy,
    )


def _draw_lines(
    gdf: gpd.GeoDataFrame,
    **kwargs,
) -> list[gv.Path]:
    """Returns a list of holoviews paths.

    This is more convoluted than it needs to be because of documented
    issues with holoviews Paths and geopandas MultiLines. See:
        https://github.com/holoviz/hvplot/issues/1107
        https://github.com/holoviz/holoviews/issues/4862
    """

    # just plot if no customization is needed
    if kwargs['c'] not in gdf.columns and 'alpha_value' not in gdf.columns:
        return gdf.hvplot(
            geo=True,
            **kwargs,
        )

    # get rid of kwargs that don't apply to lines
    kwargs.pop('legend')
    kwargs.pop('hover_cols')
    tiles = kwargs.pop('tiles')

    # otherwise, combine each line into a single plot
    line_generator = _generate_lines(
        input_dict=DynamicLineInput(
            gdf=gdf,
            args=kwargs,
            apply_alpha=bool('alpha_value' in gdf.columns),
        ),
    )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for line_output in line_generator:
            futures.append(
                executor.submit(
                    _make_path,
                    line_output=line_output,
                    args_dict=kwargs,
                ),
            )

    paths: list[gv.Path] = []
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        paths.append(result)

    return gv.Overlay(paths) * getattr(gv.tile_sources, tiles)


class DynamicPlotter:
    """Uses Holo/GeoViews to make dynamic plots."""

    @classmethod
    def make_plot(
        cls,
        gdf: gpd.GeoDataFrame,
        first: Optional[bool] = True,
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
            'legend': False,
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
        gdf, arg_dict = _set_color_args(
            gdf,
            arg_dict,
        )

        # get hover columns
        arg_dict = _set_hover_args(
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

        # return plot based on geometry type
        if 'Line' not in gdf.geom_type.iloc[0]:
            # apply alpha
            if 'alpha_value' in gdf.columns:
                arg_dict['alpha'] = gv.dim('alpha_value')
            output = gdf.hvplot(
                geo=True,
                **arg_dict,
            )
        else:
            # make colorbar
            make_cbar: bool = arg_dict.pop('colorbar', False)

            if make_cbar:
                arg_dict['vmin'] = gdf[arg_dict['c']].min()
                arg_dict['vmax'] = gdf[arg_dict['c']].max()
                cbar_dict: dict = {
                    'cmap': arg_dict['cmap'],
                    'clim': (arg_dict['vmin'], arg_dict['vmax']),
                    'clabel': arg_dict['clabel'],
                    'logz': arg_dict['logz'],
                }
            output = _draw_lines(
                gdf,
                **arg_dict,
            )
            if not first:
                output = output.Path

            # add colorbar
            if arg_dict['c']:
                output = output * _make_colorbar(**cbar_dict)

        return output
