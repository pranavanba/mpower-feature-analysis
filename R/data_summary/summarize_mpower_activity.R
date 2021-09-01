library(tidyverse)
library(synapser)
library(data.table)

synapser::synLogin()

TAPPING_FEATURES <- "syn25692238"

####################################
#### instantiate github #### 
####################################
GIT_TOKEN_PATH <- "~/git_token.txt"
GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
SCRIPT_PATH <- file.path("R", "data_summary", "summarize_mpower_activity.R")

setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)


summarize_users <- function(data){
    data %>%
        dplyr::group_by(healthCode) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::group_by(healthCode, nrecords) %>%
        dplyr::summarise_if(is.numeric, 
                            list("md" = median, 
                                 "iqr" = IQR), 
                            na.rm = TRUE) %>%
        dplyr::ungroup()
}

summarize_records <- function(data){
    data %>%
        dplyr::group_by(recordId) %>%
        dplyr::summarise_if(is.numeric, 
                            list("md" = median, 
                                 "iqr" = IQR), 
                            na.rm = TRUE) %>%
        dplyr::ungroup()
}

agg_record_filename <- "mhealthtools_tapping_v2_agg_recordId.tsv"
agg_user_filename <- "mhealthtools_tapping_v2_agg_healthCodes.tsv"
feature <- synGet(TAPPING_FEATURES)$path %>% fread()

agg_record <- feature %>% 
    summarize_records() %>%
    readr::write_tsv(agg_record_filename)
file <- synapser::File(agg_record_filename, parent = "syn25999253")
synapser::synStore(file, activity = Activity(used = TAPPING_FEATURES,
                                             executed = GIT_URL)) 
agg_user <- feature %>% 
    summarize_users() %>%
    readr::write_tsv(agg_user_filename)
file <- synapser::File(agg_user_filename, parent = "syn25999253")
synapser::synStore(file, activity = Activity(used = TAPPING_FEATURES,
                                             executed = GIT_URL)) 





