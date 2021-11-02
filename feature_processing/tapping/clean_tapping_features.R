###########################################################
#' Script to clean features across mPowerV1 and mPowerV2
#' Optional: Aggregate features based on 
#' which index of the tapping features
#' 
#' @author: aryton.tediarjo@sagebase.org
############################################################
library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
library(githubr)
library(optparse)
source("utils/curation_utils.R")

synapser::synLogin()

#' Option parser 
option_list <- list(
    make_option(c("-i", "--table_id"), 
                type = "character", 
                default = "syn15673381",
                help = "Synapse ID of mPower Tapping-Activity table entity"),
    make_option(c("-f", "--feature_id"), 
                type = "character", 
                default = "syn26301264",
                help = "Features ID for tapping"),
    make_option(c("-o", "--output_filename"), 
                type = "character", 
                default = "cleaned_mhealthtools_20secs_filter_button_none_tapping_features_mpowerV2.tsv",
                help = "Output file name"),
    make_option(c("-p", "--parent_id"), 
                type = "character", 
                default = "syn25691532",
                help = "Output parent ID"),
    make_option(c("-g", "--git_token"), 
                type = "character", 
                default = "~/git_token.txt",
                help = "Path to github token for code provenance"),
    make_option(c("-n", "--provenance_name"), 
                type = "character", 
                default = NULL,
                help = "Provenance parameter for feature extraction"),
    make_option(c("-d", "--provenance_description"), 
                type = "character", 
                default = NULL,
                help = "Provenance description"),
    make_option(c("-m", "--metadata"), 
                type = "character", 
                default = "recordId, createdOn, healthCode, appVersion, phoneInfo, dataGroups, `answers.medicationTiming`",
                help = "Metadata to keep for cleaned data"),
    make_option(c("-a", "--aggregate"), 
                type = "character",
                default = NULL,
                help = "Which index to aggregate on")
)


main <- function(){
    #' get parameter from optparse
    opt_parser = OptionParser(option_list=option_list)
    opt = parse_args(opt_parser)
    
    # Global Variables
    git_url <- get_github_url(
        git_token_path = opt$git_token,
        git_repo = "arytontediarjo/mpower-feature-analysis",
        script_path = "feature_processing/tapping/clean_tapping_features.R")
    
    # Feature reference
    feature_ref <- list(
        feature_id = opt$feature_id,
        tbl_id = opt$table_id,
        output_parent_id = opt$parent_id,
        output_filename = opt$output_filename,
        git_url = git_url,
        name = opt$provenance_name,
        description = opt$provenance_description,
        metadata = opt$metadata)
    
    # get & clean metadata from synapse table
    metadata <- synTableQuery(glue::glue(
        "SELECT {metadata} FROM {table_id}",
        metadata = opt$metadata, 
        table_id = feature_ref$tbl_id))$asDataFrame() %>%
        dplyr::select(-ROW_ID, -ROW_VERSION) %>%
        curate_app_version() %>%
        curate_med_timepoint() %>%
        curate_phone_info() %>%
        remove_test_user()
    
    # merge feature with cleaned metadata
    data <- synGet(feature_ref$feature_id)$path %>% 
        fread() %>%
        dplyr::inner_join(
            metadata, by = c("recordId")) %>%
        dplyr::select(recordId, 
                      createdOn,
                      healthCode, 
                      version, 
                      build,
                      medTimepoint,
                      phoneInfo,
                      everything())
    
    # aggregate features if parameter is given
    if(!is.null(opt$aggregate)){
        agg_vec <- vectorise_optparse_string(opt$aggregate)
        data <- data %>%
            dplyr::group_by(
                across(all_of(agg_vec))) %>%
            dplyr::mutate(nrecords = n_distinct(recordId)) %>%
            dplyr::ungroup() %>%
            dplyr::group_by(across(c(all_of(agg_vec), nrecords))) %>%
            dplyr::summarise(across(matches("Inter|Drift|Taps|XY"),
                                    list("iqr" = IQR, "md" = median),
                                    na.rm = TRUE))
    }
    
    # save to synapse
    save_to_synapse(
        data = data,
        output_filename = feature_ref$output_filename, 
        parent = feature_ref$output_parent_id,
        name = feature_ref$name,
        description = feature_ref$description,
        used = c(feature_ref$tbl_id, feature_ref$feature_id),
        executed = git_url)
}

main()