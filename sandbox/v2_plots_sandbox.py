import pollution_assessment as pa
from pathlib import Path
import geopandas as gpd

PROJECT_DIR: Path = Path.cwd()

# bad code to find the right directory
if PROJECT_DIR.name != 'pollution-assessment':
    PROJECT_DIR = PROJECT_DIR.parent
GEO_DIR: Path = PROJECT_DIR / 'geography'
PA1_DATA_DIR: Path = PROJECT_DIR / 'stage1' / 'data'
PA2_DATA_DIR: Path = PROJECT_DIR / 'stage2' / 'data_output'

catch_loads_gdf = gpd.read_parquet(
    PA2_DATA_DIR / 'catch_loads_gdf.parquet')[:100]

p1 = pa.plots_v2.make_map(
    catch_loads_gdf,
    color_column='tp_loadrate',
)
p1
