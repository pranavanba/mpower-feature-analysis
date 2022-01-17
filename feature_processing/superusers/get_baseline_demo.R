library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
library(githubr)
library(optparse)
source("utils/curation_utils.R")
source("utils/helper_utils.R")

synapser::synLogin()
CONFIG_PATH <- "templates/config.yaml"
ref <- config::get(file = CONFIG_PATH)

# get git url
git_url <- get_github_url(
    git_token_path = ref$git_token_path,
    git_repo = ref$repo_endpoint,
    script_path = "feature_processing/superusers/get_baseline_demo.R")

# get metadata to filter sensor
metadata <- get_table(
    synapse_tbl = ref$demo$table_id, 
    query_params = "where `substudyMemberships` LIKE '%superusers%'") %>%
    dplyr::select(externalId, healthCode) %>%
    distinct()

# get demo
demo_id <- synapser::synFindEntityId(
    ref$demo$feature_extraction$output_filename,
    ref$demo$feature_extraction$parent_id)
demo <- fread(synGet(demo_id)$path) %>%
    dplyr::inner_join(metadata) %>%
    dplyr::select(externalId, healthCode, everything())


# save to synapse
save_to_synapse(
    data = demo,
    output_filename = ref$demo$baseline_superusers$output_filename, 
    parent = ref$demo$baseline_superusers$parent_id,
    name = ref$demo$baseline_superusers$provenance$name,
    description = ref$demo$baseline_superusers$provenance$description,
    used = c(ref$demo$table_id,
             demo_id),
    executed = git_url)
