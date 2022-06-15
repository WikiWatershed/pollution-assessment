# Model results for all catchments in the Delaware River Basin

These files have all of the model results for all of the HUC-12's within the Delaware River and a few of its surrounding basins.
All of the csv files are produced by running [run_gwlfe_srat_drb_v3.py](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/run_gwlfe_srat_drb_v3.py).

## File Descriptions:

- [gwlfe_metadata.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - Metadata about the GWLF-E runs for each HUC-12, including the number of years the model was run, the number of land uses, and the sediment delivery ratio used

- [gwlfe_summ_q.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - GWLF-E annual estimated flows for each HUC-12

- [gwlfe_monthly_q.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - GWLF-E monthly estimated flows for each HUC-12

- [raw_load_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - GWLF-E annual estimated nutrient loads and outflow stream concentrations for each HUC-12

- [raw_source_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - GWLF-E annual estimated nutrient loads and outflow stream concentrations for each HUC-12, broken down by the source of each load (land uses and point sources)

- [attenuated_load_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - Attenuated annual estimated nutrient loads and outflow stream concentrations for each HUC-12
  - These loads are reduced by attenuating the loads through the stream network.

- [attenuated_source_summaries.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - Attenuated annual estimated nutrient loads and outflow stream concentrations for each HUC-12, broken down by the source of each load (land uses and point sources)
  - These loads are reduced by attenuating the loads through the stream network.

- [catchment_loading_rates.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - Attenuated annual estimated nutrient loads for each individual catchment basin

- [catchment_sources.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - Attenuated annual estimated nutrient loads for each individual catchment basin, broken down by the source of each load (land uses and point sources)

- [reach_concentrations.csv](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage2/DRB_GWLFE/mmw_results/gwlfe_metadata.csv)
  - Attenuated annual estimated reach concentrations for the outflow of each individual catchment basin
