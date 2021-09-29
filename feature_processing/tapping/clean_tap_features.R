library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
source("utils/curation_utils.R")

synapser::synLogin()
GIT_URL <- get_github_url(
    git_token_path = "~/git_token.txt",
    git_repo = "arytontediarjo/mpower-feature-analysis",
    script_path = "clean_tap_features.R")

FEATURE_REF <- list(
    v2 = list(
        feature_id = "syn26235452",
        tbl_id = "syn15673381",
        output_parent_id = "syn26262362",
        git_url = 
        metadata = c("recordId", 
                     "healthCode",
                     "appVersion",
                     "dataGroups",
                     "phoneInfo",
                     "'answers.medicationTiming'")
    )
)


purrr::map(FEATURE_REF, function(ref){
    metadata_str <- ref$metadata %>% 
        purrr::reduce(paste, sep = ", ")
    table <- synTableQuery(glue::glue(
        "SELECT {metadata} FROM {table_id}",
        metadata = metadata_str, 
        table_id = ref$tbl_id))$asDataFrame() %>%
        dplyr::select(-ROW_ID, -ROW_VERSION)
    synGet(ref$feature_id)$path %>% 
        fread() %>%
        dplyr::inner_join(
            table, by = c("recordId", 
                          "healthCode")) %>%
        curate_app_version("appVersion") %>%
        curate_med_timepoint("answers.medicationTiming") %>%
        curate_phone_info("phoneInfo") %>%
        remove_test_user("dataGroups")
})

    
    

