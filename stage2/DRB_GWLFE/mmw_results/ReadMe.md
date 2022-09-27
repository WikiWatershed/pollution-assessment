# Model results for all catchments in the Delaware River Basin

These files have all of the model results for all of the HUC-12's within the Delaware River and a few of its surrounding basins.
Most of the csv files are produced by running [run_gwlfe_srat_drb_v3.py](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/run_gwlfe_srat_drb_v3.py).

The csv's prefixed with "basin" are produced by running [run_whole_basin_srat.py](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/run_whole_basin_srat.py).
This condenses the HUC-12's into HUC-8's and submits all of the HUC-12's in each HUC-8 to SRAT in a single call.
The entire HUC-8 needs to be submitted together in order to get attenuation throughout the entire basin.
When each HUC-12 is submitted individually to the WikiSRAT microservice, it fails to calculate the nutrient concentrations for catchments along the main stem streams because it does not have upstream inputs for them.

## File Descriptions:

- [gwlfe_metadata.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - Metadata about the GWLF-E runs for each HUC-12, including the number of years the model was run, the number of land uses, and the sediment delivery ratio used

- [gwlfe_summ_q.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_summ_q.csv)
  - GWLF-E annual estimated flows for each HUC-12

- [gwlfe_monthly_q.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_monthly_q.csv)
  - GWLF-E monthly estimated flows for each HUC-12

- [raw_load_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/raw_load_summaries.csv)
  - GWLF-E annual estimated nutrient loads and outflow stream concentrations for each HUC-12

- [raw_source_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/raw_source_summaries.csv)
  - GWLF-E annual estimated nutrient loads and outflow stream concentrations for each HUC-12, broken down by the source of each load (land uses and point sources)

- [attenuated_load_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/attenuated_load_summaries.csv)
  - Attenuated annual estimated nutrient loads and outflow stream concentrations for each HUC-12
  - These loads are reduced by attenuating the loads through the stream network.
  - **This is not used for further analysis!**

- [attenuated_source_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/attenuated_source_summaries.csv)
  - Attenuated annual estimated nutrient loads and outflow stream concentrations for each HUC-12, broken down by the source of each load (land uses and point sources)
  - These loads are reduced by attenuating the loads through the stream network.
  - Attenuation based on only a single HUC-12
  - **This is not used for further analysis!**

- [catchment_loading_rates.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/catchment_loading_rates.csv)
  - Attenuated annual estimated nutrient loads for each individual catchment basin
  - Attenuation based on only a single HUC-12
  - **This is not used for further analysis!**

- [catchment_sources.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/catchment_sources.csv)
  - Attenuated annual estimated nutrient loads for each individual catchment basin, broken down by the source of each load (land uses and point sources)
  - Attenuation based on only a single HUC-12
  - **This is not used for further analysis!**

- [reach_concentrations.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/reach_concentrations.csv)
  - Attenuated annual estimated reach concentrations for the outflow of each individual catchment basin
  - Attenuation based on only a single HUC-12
  - **This is not used for further analysis!**
