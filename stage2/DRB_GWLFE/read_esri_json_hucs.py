#%%
from jmespath import search
import pandas as pd
import geopandas as gpd
import requests
import json
from shapely import Point

# %%
print("Reading the HUC-12 names and shapes")
huc_shapes = gpd.read_file(
    "R:\\WilliamPenn_Delaware River\\PollutionAssessment\\Stage2\\DRB_GWLFE\\HUC12s in 020401, 020402, 020403 v2.json",
)
huc_shapes = huc_shapes.set_crs("EPSG:3857").to_crs("EPSG:4326")

#%%
# Searching for boundary ID's
for idx, huc in huc_shapes.iloc[0:30].iterrows():
    search_results = []
    for search_text in [huc["huc12"], huc["name"]]:
        search_results.extend(
            requests.get(
                "{}/{}/{}".format(
                    "https://modelmywatershed.org",
                    "mmw",
                    "modeling/boundary-layers-search",
                ),
                params={"text": search_text},
                headers={
                    "Content-Type": "application/json",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": "https://staging.modelmywatershed.org/draw",
                },
            ).json()["suggestions"]
        )
    dedupped=[dict(t) for t in {tuple(d.items()) for d in search_results}]
    huc12_results = [result for result in dedupped if result["code"] == "huc12"]
    best_result = {}
    for result in huc12_results:
        if (
            (result["text"] == huc["huc12"])
            or (result["text"] == huc["huc12"] + "-" + huc["name"])
            or (len(huc12_results) == 1 and result["text"] == huc["name"])
        ):
            best_result = result
            break
    if best_result != {}:
        if pd.isna(huc["wkaoi_id"]):
            print()
            print("Got match for {}".format(huc["name"]))
            print(json.dumps(best_result, indent=2))
        if pd.notna(huc["wkaoi_id"]) and str(best_result["id"]) != str(huc["wkaoi_id"]):
            print()
            print("#########DISCREPANCY!!!#########")
            print(huc["name"], huc["wkaoi_id"])
            print(json.dumps(best_result, indent=2))
        else:
            huc_shapes.loc[idx, "wkaoi_id"] = str(best_result["id"])
            huc_shapes.loc[idx, "x"] = str(best_result["x"])
            huc_shapes.loc[idx, "y"] = str(best_result["y"])
    else:
        print()
        print(huc["name"])
        print("\t No matching result")
        print(json.dumps(huc12_results, indent=2))

#%%
huc_shapes["wkaoi_id"] = huc_shapes["wkaoi_id"].astype(int)
huc_shapes["x"] = huc_shapes["x"].astype(float)
huc_shapes["y"] = huc_shapes["y"].astype(float)
huc_shapes["wkaoi"] = (
    huc_shapes["wkaoi_code"] + "__" + huc_shapes["wkaoi_id"].apply(str)
)


#%%
print("Saving the Area of Interest data...")
# %%
huc_shapes[
    [
        "OBJECTID",
        "areaacres",
        "areasqkm",
        "states",
        "huc12",
        "name",
        "hutype",
        "humod",
        "tohuc",
        "wkaoi_id",
    ]
].to_csv(
    "R:\\WilliamPenn_Delaware River\\PollutionAssessment\\Stage2\\DRB_GWLFE\\HUC12s in 020401, 020402, 020403 v2.csv"
)

print("DONE!")

# %%
