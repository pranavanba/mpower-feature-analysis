##########################################
#' Code to join updrs scores 
#' with guid and table externalId
#' @maintainer: aryton.tediarjo@sagebase.org
##########################################

library(tidyverse)
library(reticulate)
library(data.table)
library(ggpubr)
library(patchwork)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

MAPPING <- "syn17015960"
MDS_UPDRS <- "syn25050919"
OUTPUT_PARENT_ID <- "syn25421183"
OUTPUT_FILE <- "updrs_scores.tsv"

mapping <- syn$tableQuery(glue::glue(
    "SELECT  distinct healthCode, externalId FROM {MAPPING} \
    where externalId is not null"))$asDataFrame()
ahpd_scores <- syn$get(MDS_UPDRS)$path %>% fread()

mapping %>%
    dplyr::inner_join(
        ahpd_scores, 
        by = c("externalId" = "guid")) %>%
    readr::write_tsv(OUTPUT_FILE) %>%
    save_to_synapse(syn = syn,
                    synapseclient = synapseclient,
                    data = ., 
                    output_filename = OUTPUT_FILE,
                    parent = OUTPUT_PARENT_ID,
                    used = c(MDS_UPDRS,MAPPING))