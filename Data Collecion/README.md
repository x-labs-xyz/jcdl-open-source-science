# Data Collection 
This folder contains the scripts that were used to collect the data required for analysis. We used a centralized MongoDB dataset for all data operating during the data collection stage. While we cannot provide public access to the deployed database we used, we have provided the backup image which can be imported into any MongoDB server. `database_operations.py` contains the code for operations on this Mongo dataset, while `database_interface.py` contains the code to traverse through the databse and export csv/json file for analysis.

##  Papers

- We started off with [DBLP xml file](https://dblp.uni-trier.de/xml/), and parsed it to retrieve URLS for the papers of the conferences we wanted. The code for this is present in `dblp_parsing.py`. 
- We designed scrapers for each conference of interest that would go through the links extracted from DBLP, parse through the webpage and extract relevant link to the PDF of the paper. This code is present in `scrapers.py`
- We downloaded the PDF and regex search for Github/Gitlab links within the paper in `pdf_miner.py`

## Citations
- We collected citation statistic for each paper from Microsoft Academic, Semantic Scholar and Scopus. The code for this present in `citations.py`.  For the final analysis, we stuck to Google Scholar due to its extensively larger coverage.

## Repositories
- We initially collected one-dimensional statistics for Github/Gitlab repositories. The code for this is present in `sourcecode.py`.
- After expanding the scope of the work to include time-series, we collected additional time-based data from these repositories, the code for which is contained in `detailed_sourcecode.py`.
