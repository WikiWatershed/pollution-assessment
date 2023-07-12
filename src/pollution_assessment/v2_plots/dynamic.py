import warnings
import geopandas as gpd
import matplotlib
import hvplot.pandas
import geoviews as gv
from pollution_assessment.v2_plots.shared import (
    CRS_Info,
    get_crs_info,
    add_kwargs,
)


class DynamicPlotter:
    """Uses Holo/GeoViews to make dynamic plots."""

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
