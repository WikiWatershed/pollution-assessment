# DRWI Pollution Assessment

This repository contains a collection of Python scripts and Jupyter Notebooks used to calculate an assessment of nutrient and sediment pollution for the Delaware River Watershed Initiative (DRWI). These scripts first access data and model results from DRWI-supported system, including: Model My Watershed, Stream Reach Assessment Tool (SRAT) and related systems, and FieldDoc. The scripts then use those data as input for a specific series of calculations designed to quantify how much excess nonpoint source pollution has been reduced by conservation activities and how much is remaining for each DRWI Focus Area.

This work was funded by the William Penn Foundation (WPF).

## Stage 1 DRWI Pollution Assessment: Rapid Assessment

The files in the [stage1]/(stage1/) folder were used to generate rapid assessment results and figures for [DRWI Stage 1 Pollution Assessment Report](https://docs.google.com/document/d/15YHDevp93MJGsngxUvodJbf7kXwgkgU4LgBXOAt35Rc/edit?usp=sharing), submitted to the William Penn Foundation on October 29, 2021 and revised on February 7, 2022.

## Stage 2 DRWI Pollution Assessment: Refined Assessment

The files in the [stage2]/(stage2/) folder were used to generate refined assessment results and figures for presentation to WPF by September of 2022.

## Get Started

### Install with Conda

Follow these steps to install using the [conda](https://docs.conda.io/en/latest/) package manager.

#### 1. Install the Anaconda Python Distribution

We recommend installing the [latest release](https://docs.anaconda.com/anaconda/reference/release-notes/) of [**Anaconda Individual Edition**](https://www.anaconda.com/distribution), which includes the conda, a complete Python (and R) data science stack, and the helpful Anaconda Navigator GUI.
- Follow [Anaconda Installation](https://docs.anaconda.com/anaconda/install/) documentation.

A lighter-weight alternative is to install [Miniconda](https://docs.conda.io/en/latest/miniconda.html).

#### 2. Clone or Download this Repository

From this Github page, click on the green "Code" dropdown button near the upper right. Select to either "Open in GitHub Desktop" (i.e. git clone) or "Download ZIP". We recommend using GitHub Desktop, to most easily receive updates.

Place your copy of this repo in any convenient location on your computer.

#### 3. Create a Conda Environment for this Repository (optional)

Although HSP2 can be run from the default `base` environment created by Anaconda, we recommend creating a custom environment that includes the exact combination of software dependencies that we've used in development and testing.

Create the `drwi_pa` environment from our [`environment.yml`](environment.yml) file, which lists all primary dependencies, using one of these approaches: 
1. Use the **Import** button on [Anaconda Navigator's Environments tab](https://docs.anaconda.com/anaconda/navigator/overview/#environments-tab), or 
2. Use the following [`conda create`](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html#managing-environments) command in your terminal or console,  replacing `path/environment.yml` with the full file pathway to the [`environment.yml`](environment.yml) file in the local cloned repository.

    ```shell
    conda env create --file path/environment.yml
    ```
To update your environment, either use Anaconda Navigator, or run the following command:  

```shell
conda env update --file path/environment.yml --prune
```

or for a clean re-install:

```shell
conda env create --file path/environment.yml --force
```

Optionally, if you have installed [`conda-libmamba-solver`](https://conda.github.io/conda-libmamba-solver/getting-started/) into your base environment, you can also add the `--solver=libmamba` option flag to any fo the commands above.


#### 4. Add your Repo's Path to Miniconda site-packages

To have access to this repository's modules in your Python environments, it is necessary to save the path to your copy of this repo in Miniconda's or Anaconda's `conda.pth` file in the environment's `site-packages` directory (i.e. something like `<$HOME>/anaconda/lib/pythonX.X/site-packages/conda.pth` or `<$HOME>/miniconda3/envs/drwi_pa/lib/python3.11/site-packages/conda.pth` or similar), where `<$HOME>` refers to the full path of the user directory, such as `/home/username` on Linux/Mac.

- The easiest way to do this is to use the [conda develop](https://docs.conda.io/projects/conda-build/en/latest/resources/commands/conda-develop.html) command in the console or terminal like this, replacing `/path/to/module/` with the full file pathway to the local cloned HSPsquared repository:

    ```console
    conda develop /path/to/module/
    ```

You should now be able to run the Tutorials and create your own Jupyter Notebooks!

