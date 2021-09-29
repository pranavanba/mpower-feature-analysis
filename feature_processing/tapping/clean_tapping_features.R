###########################################################
#' Script to clean features across mPowerV1 and mPowerV2
#' 
#' @author: aryton.tediarjo@sagebase.org
############################################################
library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
library(githubr)
source("utils/curation_utils.R")

synapser::synLogin()

# Global Variables
GIT_URL <- get_github_url(
    git_token_path = "~/git_token.txt",
    git_repo = "arytontediarjo/mpower-feature-analysis",
    script_path = "feature_processing/tapping/clean_tapping_features.R")
FEATURE_REF <- list(
    v1 = list(
        feature_id = "syn26250287",
        tbl_id = "syn10374665",
        output_parent_id = "syn26262474",
        output_filename = "cleaned_mhealthtools_tapping_data_v1_freeze.tsv",
        git_url = GIT_URL,
        name = "clean tapping v1 data",
        description = "remove test group, curate app version, phoneInfo, and medication timepoint",
        metadata = c("recordId", 
                     "healthCode",
                     "appVersion",
                     "dataGroups",
                     "phoneInfo",
                     "medTimepoint")
    ),
    v2 = list(
        feature_id = "syn26235452",
        tbl_id = "syn15673381",
        output_parent_id = "syn26262362",
        output_filename = "cleaned_mhealthtools_tapping_data_v2.tsv",
        git_url = GIT_URL,
        name = "clean tapping v2 data",
        description = "remove test group, curate app version, phoneInfo, and medication timepoint",
        metadata = c("recordId", 
                     "healthCode",
                     "appVersion",
                     "dataGroups",
                     "phoneInfo",
                     "'answers.medicationTiming'")
    )
)

main <- function(){
    # Map feature reference
    purrr::map(FEATURE_REF, function(ref){
        # parse vector to string query
        metadata_str <- ref$metadata %>% 
            purrr::reduce(paste, sep = ", ")
        
        # get & clean metadata from synapse table
        metadata <- synTableQuery(glue::glue(
            "SELECT {metadata} FROM {table_id}",
            metadata = metadata_str, 
            table_id = ref$tbl_id))$asDataFrame() %>%
            dplyr::select(-ROW_ID, -ROW_VERSION) %>%
            curate_app_version() %>%
            curate_med_timepoint() %>%
            curate_phone_info() %>%
            remove_test_user()
        
        # merge feature with cleaned metadata
        data <- synGet(ref$feature_id)$path %>% 
            fread() %>%
            dplyr::inner_join(
                metadata, by = c("recordId", 
                                 "healthCode"))
        
        # save to synapse
        save_to_synapse(
            data = data,
            output_filename = ref$output_filename, 
            parent = ref$output_parent_id,
            name = ref$name,
            description = ref$description,
            used = c(ref$tbl_id, ref$feature_id),
            executed = ref$git_url)
    })
}

main()


    
    

