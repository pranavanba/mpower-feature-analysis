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
                default = "syn12514611",
                help = "Synapse ID of mPower Walking-Activity table entity"),
    make_option(c("-f", "--feature_id"), 
                type = "character", 
                default = "syn26434900",
                help = "Features ID for tapping"),
    make_option(c("-o", "--output_filename"), 
                type = "character", 
                default = "cleaned_pdkit_rotation_walk30secs_v2_features.tsv",
                help = "Output file name"),
    make_option(c("-p", "--parent_id"), 
                type = "character", 
                default = "syn26341967",
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

aggregate_walk <- function(data, agg_vec){
   data %>%
        dplyr::group_by(
            across(all_of(agg_vec))) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::ungroup() %>%
        dplyr::group_by(across(c(all_of(agg_vec), nrecords))) %>%
        dplyr::summarise(across(matches("^x|^y|^z|^AA|^rotation"),
                                list("iqr" = IQR, "md" = median),
                                na.rm = TRUE))
}


main <- function(){
    #' get parameter from optparse
    opt_parser <- OptionParser(option_list=option_list)
    opt <- parse_args(opt_parser)
    
    # Global Variables
    git_url <- get_github_url(
        git_token_path = opt$git_token,
        git_repo = "arytontediarjo/mpower-feature-analysis",
        script_path = "feature_processing/walk30secs/clean_walk30secs_features.R")
    
    # Feature reference
    feature_ref <- list(
        feature_id = opt$feature_id,
        tbl_id = opt$table_id,
        demo_id = 'syn26601401',
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
    
    # get demo
    demo <- synGet(feature_ref$demo_id)$path %>%
        fread(.) %>%
        dplyr::select(healthCode, age, sex, diagnosis)
    
    # merge feature with cleaned metadata
    data <- synGet(feature_ref$feature_id)$path %>% 
        fread() %>%
        dplyr::inner_join(
            metadata, by = c("recordId")) %>%
        dplyr::inner_join(
            demo, by = c("healthCode")) %>%
        dplyr::select(recordId, 
                      createdOn,
                      healthCode, 
                      version, 
                      build,
                      medTimepoint,
                      phoneInfo,
                      age,
                      sex,
                      diagnosis,
                      everything())
    
    # aggregate features if parameter is given
    if(!is.null(opt$aggregate)){
        agg_vec <- vectorise_optparse_string(opt$aggregate)
        
        feature_list <- list()
        # get non rotation segments:
        feature_list$non_rotation <- data %>%
            dplyr::filter(is.na(rotation_omega)) %>%
            dplyr::select(-rotation_omega)
        
        # get rotation segments:
        feature_list$rotation <- data %>%
            dplyr::filter(!is.na(rotation_omega)) %>%
            dplyr::select(healthCode, 
                          recordId, 
                          medTimepoint,
                          rotation_omega)
        
        # map through aggregated features
        agg_features <- purrr::map(
            feature_list, ~.x %>% 
                aggregate_walk(c("healthCode")) %>%
                dplyr::ungroup()) %>%
            purrr::reduce(dplyr::left_join, 
                          by = c("healthCode")) %>%
            dplyr::mutate() %>%
            dplyr::select(
                healthCode,
                nrecords = nrecords.x, 
                everything(),
                -nrecords.y)
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