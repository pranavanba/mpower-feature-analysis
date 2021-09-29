library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(reticulate)
library(MatchIt)
source("R/utils/analysis_utils.R")
source("R/utils/utils.R")

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### instantiate github #### 
####################################
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "match_healthcodes.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)

####################################
#### instantiate global variables #### 
####################################
OUTPUT_REF <- list(
    walk = list(
        output_file = "age_gender_matched_healthcodes_walk.tsv",
        id = "syn25782772",
        parent_id = "syn25782930"),
    tap = list(
        output_file = "age_gender_matched_healthcodes_tap.tsv",
        id = "syn25782775",
        parent_id = "syn25782930"),
    rest = list(
        output_file ="age_gender_matched_healthcodes_rest.tsv",
        id = "syn25782776",
        parent_id = "syn25782930")
)


purrr::walk(names(OUTPUT_REF), function(activity){
    agg_features <- syn$get(OUTPUT_REF[[activity]])$path %>% fread()
    agg_features %>% 
        match_healthcodes() %>%
        save_to_synapse(
            output_file = OUTPUT_REF[[activity]]$output_file,
            parent_id = OUTPUT_REF[[activity]]$parent_id,
            source_tbl = OUTPUT_REF[[activity]]$id,
            executed = GIT_URL, 
            activity_name = "aggregate features")
})


