# build base image
FROM rocker/tidyverse:4.0.0

RUN apt-get update -y\
    && apt-get install -y python3-dev\
    && apt-get install -y python3-venv\
    && apt-get install -y git

## R dependencies
RUN R -e 'install.packages("renv")'
