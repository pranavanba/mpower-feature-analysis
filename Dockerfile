# build base image
FROM rocker/tidyverse:4.0.0

RUN apt-get update -y\
    && apt-get install -y python3-dev\
    && apt-get install -y python3-venv\
    && apt-get install -y git
## R dependencies
RUN R -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org"))'\
    && R -e 'install.packages("doMC")'\
    && R -e 'devtools::install_github("brian-bot/githubr")'\
    && R -e 'install.packages("reticulate")'\
    && R -e 'devtools::install_github("Sage-Bionetworks/mhealthtools")'\
    && R -e 'install.packages("plyr")'\
    && R -e 'install.packages("jsonlite")'

## python dependencies
RUN python3 -m venv ~/env\
    && . ~/env/bin/activate \
    && pip install wheel\
    && pip install psutil\
    && pip install synapseclient\
    && pip install git+https://github.com/arytontediarjo/PDKitRotationFeatures.git


