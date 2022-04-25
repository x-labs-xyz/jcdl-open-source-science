# Analysis
This folder contains the code used for all inference and analysis work in the paper. `Analysis.ipynb` contains the crux of the work, including explorative analysis of the dataset, tests of statistical significance of citation distributions of papers with and without a Github repository, and the training and evaluation of a classification model that predicts if a paper is highly cited based entirely on its open source repository features.

# Dataset
`detailed_commits_all.csv` is the processed dataset containing selected features from all papers in the dataset. This is the output of the `databse_interface.py` in the Data Collection section. 

# Quality Control tests
We selected a statistically significant sample of the dataset and manually test the link between paper and Github repository as established by our code. `manually_tested_samples.csv` contains the result of this exploration, the result of which are mentioned in the paper. Additionally, we also automated the test for the entire dataset.
### Automated Quality Control 
`Data curation test.ipynb` contains the code used for automated quality control to ensure papers and Github repositories are matched properly. The result from the automated test is present in `automated_tested_sample.csv`. 

# Classification Output
The csv files contain the accuracy, precision and ROC metrics of the gridsearched hybrid cluster-then-classify models. The file name represent the year and the clustering method used.For instance `2015_kshape.csv` represents papers from 2015, classified using k-Shape clustering.