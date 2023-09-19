import warnings
import cartopy
import pyproj
from typing import (
    TypedDict,
)


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


def add_kwargs(
    kwargs: dict,
    other_kwarg: dict,
    warn: bool,
) -> dict:
    """Adds other_kwarg k, v pairs kwargs:dict, with warnings for conflicts."""
    for k, v in other_kwarg.items():
        if k in kwargs and warn and kwargs.get(k, None) != v:
            warnings.warn(
                f'Kwarg conflict! Overriding default {k}={kwargs[k]} with {k}={v}.',
            )
        kwargs[k] = v
    return kwargs
