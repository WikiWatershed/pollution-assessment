Stage 1 DRWI Pollution Assessment
===

The files in this folder were used to generate the results and figures for [DRWI Stage 1 Pollution Assessment Report](https://docs.google.com/document/d/15YHDevp93MJGsngxUvodJbf7kXwgkgU4LgBXOAt35Rc/edit#), submitted to the William Penn Foundation on October 29, 2021 and revised on February 7, 2022.

These files were originally developed within the [TheAcademyofNaturalSciences/WikiSRATMicroService](https://github.com/TheAcademyofNaturalSciences/WikiSRATMicroService) library, and were moved here in April 2022. The full commit history of that work has been archived in the [archive_PollutionAssessmentStage1](https://github.com/TheAcademyofNaturalSciences/WikiSRATMicroService/tree/archive_PollutionAssessmentStage1) branch.

The main workflow requires running these Jupyter notebooks in this order:
- [WikiSRAT_Demo.ipynb](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage1/WikiSRAT_Demo.ipynb)
    - This first notebook fetches and prepares all the data necessary for the Stage 1 Assessment.
    - NOTE that running the first half of this notebook requires a private key for direct access the databases used by [WikiSRATMicroService](https://github.com/TheAcademyofNaturalSciences/WikiSRATMicroService), so this workflow cannot be fully replicated by the general public. However, data files required to run the second notebook have been saved in the [stage1/data](https://github.com/WikiWatershed/pollution-assessment/tree/main/stage1/data) folder.
- [WikiSRAT_AnalysisViz.ipynb](https://github.com/WikiWatershed/pollution-assessment/blob/main/stage1/WikiSRAT_AnalysisViz.ipynb)
    - This second notebook analyzes and visualizes the data for the Stage 1 Assessment.
    - NOTE that this notebook can be run based on archived datasets in the [stage1/data](https://github.com/WikiWatershed/pollution-assessment/tree/main/stage1/data) folder.

All other notebooks were for exploratory work.
