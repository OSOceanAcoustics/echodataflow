# Echodataflow on Local Machine

Welcome to this beginner-friendly notebook that guides you through the process of using `echodataflow` to process EK60 data from the SH1707 survey. `echodataflow` is a powerful tool for ocean acoustics data processing. In this notebook, we'll walk through the following steps:

1. **Setting Up**: Importing necessary libraries and setting up the configuration paths for the dataset and pipeline.

2. **Getting Data**: Using `glob_url` to retrieve a list of URLs matching the specified pattern of raw EK60 data files.

3. **Preparing Files**: Extracting file names from URLs and creating a file listing for the transect.

4. **Processing with echodataflow**: Starting the echodataflow processing using the specified configurations.

5. **Results**: Displaying the first entry from the processed data.

Let's get started!

```{tableofcontents}
```
