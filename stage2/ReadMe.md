Stage 2 DRWI Pollution Assessment: Refined Assessment
===

The files in the [stage2]/(stage2/) folder were used to generate refined assessment results and figures for presentation to WPF by September of 2022.

The files improve and extend the work done for the Stage 1 Rapid Assessment.


The main workflow requires running these Jupyter notebooks in this order:

- [PA2_1_FetchData.ipynb](stage2/PA2_1_FetchData.ipynb)
    - This first notebook fetches and prepares all the input data and modeling necessary for the Stage 2 Assessment.
    - NOTE that much of this data is first fetched from DRWI web service APIs and saved as CSV files in stage2 subfolders.
- [PA2_2_Analysis.ipynb](stage2/PA2_2_Analysis.ipynb)
    - This second notebook analyzes the data for the Stage 2 Assessment, performing the calculations for excess nonpoint source pollution
- [PA2_3_Viz.ipynb](stage2/PA2_3_Viz.ipynb)
    - This thir notebook visualizes pollution assessment findings, including creating hotspot maps.


