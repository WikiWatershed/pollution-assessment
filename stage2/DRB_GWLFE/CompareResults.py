#%%
import pandas as pd
import numpy as np

#%%
# Set up the API client
from mmw_secrets import (
    csv_path,
)

frames = []

# Subbasins, starting as single huc12
sb_load_sum_results = pd.read_csv(csv_path + "gwlfe_sb_load_summaries.csv")
frames.append(sb_load_sum_results)

sb_source_loads_results = pd.read_csv(csv_path + "gwlfe_sb_source_summaries.csv")
frames.append(sb_source_loads_results)

srat_rates = pd.read_csv(csv_path + "srat_catchment_load_rates.csv")
frames.append(srat_rates)

srat_conc = pd.read_csv(csv_path + "srat_catchment_load_rates.csv")
frames.append(srat_conc)


# whole (non-subbasin)
whole_metas_results = pd.read_csv(csv_path + "gwlfe_whole_metadata.csv")
frames.append(whole_metas_results)

whole_sum_results = pd.read_csv(csv_path + "gwlfe_whole_summ_q.csv")
frames.append(whole_sum_results)

whole_monthly_results = pd.read_csv(csv_path + "gwlfe_whole_monthly_q.csv")
frames.append(whole_monthly_results)

whole_load_sum_results = pd.read_csv(csv_path + "gwlfe_whole_load_summaries.csv")
frames.append(whole_load_sum_results)

whole_source_loads_results = pd.read_csv(csv_path + "gwlfe_whole_source_summaries.csv")
frames.append(whole_source_loads_results)

#%%
for frame in frames:
    for col in ["huc", "huc10", "huc12", "huc_run"]:
        if col in frame.columns:
            frame[col] = frame[col].astype(str)
            frame.loc[~frame[col].str.startswith("0"), col] = (
                "0" + frame.loc[~frame[col].str.startswith("0")][col]
            )
    if "Unnamed: 0" in frame.columns:
        frame.drop(columns=["Unnamed: 0"], inplace=True)

# %%
load_sums = [
    sb_load_sum_results,
    whole_load_sum_results.loc[
        (whole_load_sum_results["Source"] == "Total Loads")
        & (whole_load_sum_results["huc_level"] == 12)
    ].copy(),
]
all_load_sums = (
    pd.concat(load_sums).sort_values(by=["huc", "huc_run"]).reset_index(drop=True)
)
all_load_sums["Source"] = "Entire area"

source_loads = [
    sb_source_loads_results,
    whole_source_loads_results,
]
all_source_loads = (
    pd.concat(source_loads)
    .dropna(subset=["TotalN", "TotalP", "Sediment"], how="all")
    .sort_values(by=["huc", "Source", "huc_run"])
    .reset_index(drop=True)
)

gwlfe_loads = pd.concat([all_load_sums, all_source_loads])
#%%
gwlfe_loads_t = gwlfe_loads.pivot(
    index=[
        "huc",
        "land_use_source",
        "stream_layer",
        "weather_source",
        "closest_weather_stations",
        "Source",
    ],
    columns=["gwlfe_endpoint", "huc_run_level"],
    values=["Sediment", "TotalN", "TotalP"],
)
for param in ["TotalN", "TotalP", "Sediment"]:
    gwlfe_loads_t[(param, "diff", 12)] = (
        abs(
            gwlfe_loads_t[(param, "gwlfe", 12)] - gwlfe_loads_t[(param, "subbasin", 12)]
        )
        / (
            (
                gwlfe_loads_t[(param, "gwlfe", 12)]
                + gwlfe_loads_t[(param, "subbasin", 12)]
            )
            / 2
        )
    ) * 100
    gwlfe_loads_t[(param, "subbasin", "diff")] = (
        abs(
            gwlfe_loads_t[(param, "subbasin", 12)]
            - gwlfe_loads_t[(param, "subbasin", 10)]
        )
        / (
            (
                gwlfe_loads_t[(param, "subbasin", 12)]
                + gwlfe_loads_t[(param, "subbasin", 10)]
            )
            / 2
        )
    ) * 100
gwlfe_loads_t.replace([np.inf, -np.inf], np.nan, inplace=True)
gwlfe_loads_t = gwlfe_loads_t.sort_index(axis=1)
#%%
print("Differences between total loads from subbasins/SRAT and whole shapes")
gwlfe_loads_t.loc[
    :, gwlfe_loads_t.columns.get_level_values("gwlfe_endpoint") == "diff"
].groupby(by="Source").mean()
#%%
print("Differences between total loads from subbasins when run as HUC-12 vs HUC-10")
gwlfe_loads_t.loc[
    :, gwlfe_loads_t.columns.get_level_values("huc_run_level") == "diff"
].groupby(by="Source").mean()

#%%
gwlfe_whole_sub_difs = gwlfe_loads_t.loc[
    (
        (gwlfe_loads_t[("TotalN", "diff", 12)] != 0)
        | (gwlfe_loads_t[("TotalP", "diff", 12)] != 0)
        | (gwlfe_loads_t[("Sediment", "diff", 12)] != 0)
    )
    & ~(pd.isna(gwlfe_loads_t[("Sediment", "diff", 12)]))
]
gwlfe_whole_sub_pct_difs = len(gwlfe_whole_sub_difs.index) / len(gwlfe_loads_t.index)
#%%
gwlfe_loads_t.loc[
    :, gwlfe_loads_t.columns.get_level_values("huc_run_level") == 12
].reset_index().to_csv(csv_path + "WholeSubbasinDiffs.csv")

#%%
gwlfe_sub_sub_difs = gwlfe_loads_t.loc[
    (
        (gwlfe_loads_t[("TotalN", "subbasin", "diff")] != 0)
        | (gwlfe_loads_t[("TotalP", "subbasin", "diff")] != 0)
        | (gwlfe_loads_t[("Sediment", "subbasin", "diff")] != 0)
    )
    & ~(pd.isna(gwlfe_loads_t[("Sediment", "subbasin", "diff")]))
]
gwlfe_sub_sub_pct_difs = len(gwlfe_sub_sub_difs.index) / len(gwlfe_loads_t.index)


# %%
srat_rates_t = srat_rates.drop_duplicates(
    subset=[
        # "land_use_source",
        # "stream_layer",
        # "weather_source",
        # "closest_weather_stations",
        "catchment",
        "huc_run_level",
        "Sediment",
        "TotalN",
        "TotalP",
    ]
).pivot(
    index=[
        # "land_use_source",
        # "stream_layer",
        # "weather_source",
        # "closest_weather_stations",
        "catchment",
    ],
    columns=["huc_run_level"],
    values=["Sediment", "TotalN", "TotalP"],
)
#%%
srat_rates_t.loc[
    (pd.isna(srat_rates_t[("TotalP", 12)]) & pd.notna(srat_rates_t[("TotalP", 10)]))
    | (pd.notna(srat_rates_t[("TotalP", 12)]) & pd.isna(srat_rates_t[("TotalP", 10)]))
]
#%%
for param in ["TotalN", "TotalP", "Sediment"]:
    srat_rates_t[(param, "diff")] = (
        abs(srat_rates_t[(param, 12)] - srat_rates_t[(param, 10)])
        / ((srat_rates_t[(param, 12)] + srat_rates_t[(param, 10)]) / 2)
    ) * 100
srat_rates_t.replace([np.inf, -np.inf], np.nan, inplace=True)
srat_rates_t = srat_rates_t.sort_index(axis=1)
print(
    "Differences between SRAT concentrations from subbasins when run as HUC-12 vs HUC-10"
)
srat_rates_t.loc[
    :, srat_rates_t.columns.get_level_values("huc_run_level") == "diff"
].mean()

# %%
srat_conc_t = srat_conc.drop_duplicates(
    subset=[
        # "land_use_source",
        # "stream_layer",
        # "weather_source",
        # "closest_weather_stations",
        "catchment",
        "huc_run_level",
        "Sediment",
        "TotalN",
        "TotalP",
    ]
).pivot(
    index=[  # "land_use_source",
        # "stream_layer",
        # "weather_source",
        # "closest_weather_stations",
        "catchment",
    ],
    columns=["huc_run_level"],
    values=["Sediment", "TotalN", "TotalP"],
)
srat_conc_t.loc[
    (pd.isna(srat_conc_t[("TotalP", 12)]) & pd.notna(srat_conc_t[("TotalP", 10)]))
    | (pd.notna(srat_conc_t[("TotalP", 12)]) & pd.isna(srat_conc_t[("TotalP", 10)]))
]

#%%
for param in ["TotalN", "TotalP", "Sediment"]:
    srat_conc_t[(param, "diff")] = (
        abs(srat_conc_t[(param, 12)] - srat_conc_t[(param, 10)])
        / ((srat_conc_t[(param, 12)] + srat_conc_t[(param, 10)]) / 2)
    ) * 100
srat_conc_t.replace([np.inf, -np.inf], np.nan, inplace=True)
srat_conc_t = srat_conc_t.sort_index(axis=1)
print(
    "Differences between SRAT concentrations from subbasins when run as HUC-12 vs HUC-10"
)
srat_conc_t.loc[
    :, srat_conc_t.columns.get_level_values("huc_run_level") == "diff"
].mean()

# %%
