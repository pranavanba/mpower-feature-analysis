# build base image
FROM rocker/tidyverse:4.0.0

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y python3 python3-venv python3-pip git
    
## Clone git repo
RUN git clone https://github.com/pranavanba/mpower-feature-analysis

## Change work dir
WORKDIR mpower-feature-analysis

## Pull updates
RUN git pull

## Python dependencies
RUN python3 -m pip install --user virtualenv

RUN python3 -m venv ~/env && \
    . ~/env/bin/activate

RUN python3 -m pip install synapseclient
RUN python3 -m pip install wheel
RUN python3 -m pip install git+https://github.com/arytontediarjo/PDKitRotationFeatures.git

## Install R packages
RUN R -e 'install.packages("reticulate")'
RUN R -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org"))'
RUN R -e "install.packages('remotes')"
RUN R -e "remotes::install_github('Sage-Bionetworks/mhealthtools', force = TRUE)"
