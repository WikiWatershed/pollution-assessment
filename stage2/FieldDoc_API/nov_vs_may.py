# %%
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd

# %%
# Find your current working directory, which should be folder for this notebook.
Path.cwd()
# Set your project directory to your local folder for your clone of this repository
project_path = Path.cwd().parent
project_path

# %%
# Files created by Sara Damiano on 2023-05-16
# These are data straight out of FieldDocs
print("2023-05-16 Protection Data")
fd_protec_gdf_20230516 = gpd.read_parquet(
    project_path / Path("private/protection_bmps_from_FieldDoc_20230516.parquet")
)
fd_protec_gdf_20230516["created_at"] = pd.to_datetime(
    fd_protec_gdf_20230516["created_at"]
)
fd_protec_gdf_20230516["modified_at"] = pd.to_datetime(
    fd_protec_gdf_20230516["modified_at"]
)
print(len(fd_protec_gdf_20230516.index))
fd_protec_gdf_20230516[["created_at", "modified_at"]].agg([np.min, np.max])

# %%
# Files created by Sara Damiano on 2022-11-02
# These are data straight out of FieldDocs
print("2022-11-02 Protection Data")
fd_protec_gdf_20221102 = gpd.read_parquet(
    project_path / Path("private/protection_bmps_from_FieldDoc_20221102.parquet")
)
fd_protec_gdf_20221102["created_at"] = pd.to_datetime(
    fd_protec_gdf_20221102["created_at"]
)
fd_protec_gdf_20221102["modified_at"] = pd.to_datetime(
    fd_protec_gdf_20221102["modified_at"]
)
print(len(fd_protec_gdf_20221102.index))
fd_protec_gdf_20221102[["created_at", "modified_at"]].agg([np.min, np.max])

# %%
# Files created by Sara Damiano on 2023-05-16
# These are data straight out of FieldDocs
print("2023-05-16 Restoration Data")
fd_rest_gdf_20230516 = gpd.read_parquet(
    project_path / Path("private/restoration_bmps_from_FieldDoc_20230516.parquet")
)
fd_rest_gdf_20230516["created_at"] = pd.to_datetime(fd_rest_gdf_20230516["created_at"])
fd_rest_gdf_20230516["modified_at"] = pd.to_datetime(
    fd_rest_gdf_20230516["modified_at"]
)
print(len(fd_rest_gdf_20230516.index))
# print(len(fd_rest_gdf_20230516.loc[fd_rest_gdf_20230516["created_at"]<pd.Timestamp("2022-11-02 11:47-05:00")].index))
# print(len(fd_rest_gdf_20230516.loc[fd_rest_gdf_20230516["modified_at"]<pd.Timestamp("2022-11-02 11:47-05:00")].index))
# print(len(fd_rest_gdf_20230516.loc[fd_rest_gdf_20230516["created_at"]<pd.Timestamp("2022-10-03 19:07:18.340617+00:00")].index))
# print(len(fd_rest_gdf_20230516.loc[fd_rest_gdf_20230516["modified_at"]<pd.Timestamp("2022-10-10 20:15:04.977145+00:00")].index))
fd_rest_gdf_20230516[["created_at", "modified_at"]].agg([np.min, np.max])

# %%
# Files created by Sara Damiano on 2022-11-02
# These are data straight out of FieldDocs
print("2022-11-02 Restoration Data")
fd_rest_gdf_20221102 = gpd.read_parquet(
    project_path / Path("private/restoration_bmps_from_FieldDoc_20221102.parquet")
)
fd_rest_gdf_20221102["created_at"] = pd.to_datetime(fd_rest_gdf_20221102["created_at"])
fd_rest_gdf_20221102["modified_at"] = pd.to_datetime(
    fd_rest_gdf_20221102["modified_at"]
)
print(len(fd_rest_gdf_20221102.index))
fd_rest_gdf_20221102[["created_at", "modified_at"]].agg([np.min, np.max])


# %%
new_since_nov = fd_rest_gdf_20230516.loc[
    ~fd_rest_gdf_20230516["practice_id"].isin(
        fd_rest_gdf_20221102["practice_id"].unique()
    )
]
deleted_since_nov = fd_rest_gdf_20221102.loc[
    ~fd_rest_gdf_20221102["practice_id"].isin(
        fd_rest_gdf_20230516["practice_id"].unique()
    )
]

nov_may_merge = fd_rest_gdf_20230516.merge(
    fd_rest_gdf_20221102[["practice_id", "created_at", "modified_at"]].rename(
        columns={"created_at": "created_asof_nov", "modified_at": "modified_asof_nov"}
    ),
    how="outer",
    on="practice_id",
    indicator=True,
)
projects_in_both = nov_may_merge.loc[nov_may_merge["_merge"] == "both"][
    "project_id"
].unique()
new_projects = new_since_nov.loc[~new_since_nov["project_id"].isin(projects_in_both)][
    "project_id"
].nunique()
updated_projects = new_since_nov.loc[
    new_since_nov["project_id"].isin(projects_in_both)
]["project_id"].nunique()

print(
    f"May Restoration Total Practices: {len(fd_rest_gdf_20230516.index)}, Distinct Projects: {fd_rest_gdf_20230516['project_id'].nunique()}"
)
print(
    f"November Restoration Total Practices: {len(fd_rest_gdf_20221102.index)}, Distinct Projects: {nov_may_merge.loc[nov_may_merge['_merge']=='both']['project_id'].nunique()}"
)
print(
    f"Added since Nov: Practices: {len(new_since_nov.index)}, Projects: New: {new_projects}, Updated: {updated_projects}"
)
print(
    f"Deleted since Nov: Practices: {len(deleted_since_nov.index)}, Projects: Unknown"
)

# %%
