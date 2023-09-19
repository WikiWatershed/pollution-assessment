# package version
__version__ = '0.1.0'

# populate package namespace
from pollution_assessment import (
    calc,
    plot,
    dynamic_plot,
    plot_protected_land,
    summary_stats,
)

from pollution_assessment.v2_plots.make_map import (
    make_map,
)
