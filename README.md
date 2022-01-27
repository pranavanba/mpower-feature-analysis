# mPower Feature Analysis ETL
Maintainter: aryton.tediarjo@sagebase.org, larssono@sagebase.org

### About
This repository is used as a ETL wrapper for fetching mPower features based on different feature extraction tools. 
Here are our current supported tools in this ETL Github repository:
- [Mhealthtools](https://github.com/Sage-Bionetworks/mhealthtools/blob/master/R/get_tapping_features.R)
- [PDKit](https://github.com/pdkit/pdkit)

### Setting up Environment
This repository will be using Docker and [R's Renv package management](https://rstudio.github.io/renv/articles/renv.html) to create the environment. 
Note: Repo version will be updated alongside any changes in the Docker Image

#### 1. Clone Repository
```zsh
git clone https://github.com/arytontediarjo/mpower-feature-analysis.git <PROJECT_NAME>
cd <PROJECT_NAME>
docker build --no-cache -t <IMAGE_NAME> .
```

#### 2. Create Container Resources
```zsh
docker -d run <IMAGE_NAME> 
```

#### 3. Caching Credentials
We will be using two authentication processes that will be done:
- Synapse Authentication
- Github Authentication

#### a. Check your container ID
```zsh
docker ps -a
```
#### b. Fetch the container ID from `docker build` tags, and pass it to:
```zsh
docker make authenticate\
PARAMS="utils/authenticate.R\
  -u <SYNAPSE_USERNAME>\
  -p <SYNAPSE_PASSW>\
  -g <GITHUB_TOKEN>"
```
This will create an environment with your necessary credentials to run the analysis.


#### 4. Running the Analysis
To be Updated with Nextflow

