library(tidyverse)
library(githubr)
library(data.table)
library(furrr)
library(future)
source("utils/helper_utils.R")

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$Synapse()
syn$login()
syn$table_query_timeout <- 9999999

GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("feature_processing", 
                         "tremor", 
                         "clean_tremor_features.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)

####################################
# instantiate named list variable #
####################################

# map kinetic measurement type to abbreviations
KINETIC_MAPPING <- list(
    acceleration = c(
        "acceleration" = "ua",
        "jerk" = "uj",
        "displacement" = "ud",
        "velocity" = "uv",
        "acf" = "uaacf"),
    gyroscope = c(
        "acceleration" = "uaa",
        "jerk" = "uaj",
        "displacement" = "uad",
        "velocity" = "uav",
        "acf" = "uavcf"
    ))

# get mapping for outputs
OUTPUT_REF <- list(
    agg_hc = "mhealthtools_tremor_v2_agg_healthcodes.tsv",
    agg_record = "mhealthtools_tremor_v2_agg_recordId.tsv",
    feat_id = "syn26215339",
    best_features_id = "syn17088603",
    tbl_id = "syn12977322",
    demo_id = "syn26601401",
    parent_id = "syn26341966",
    funs = "summarize_tremor"
)


#' function to get top n features
top_n_features <- function(syn, best_features_id, n = 30){
    syn$get(best_features_id)$path %>% 
        fread() %>%
        dplyr::filter(assay == "rest") %>%
        dplyr::slice(1:n) %>% 
        .$Feature %>%
        glue::glue_collapse(sep = "|")
}

#' function to get table and parse 
#' medication columns accordingly
get_table <- function(syn, tbl_id){
    syn$tableQuery(
        glue::glue("SELECT * FROM {id}", id = tbl_id))$asDataFrame() %>%
        dplyr::rename_with(
            .cols = matches("answers.medicationTiming"), 
            .fn = ~"medTimepoint") %>%
        dplyr::mutate(medTimepoint = unlist(medTimepoint)) %>%
        dplyr::select(recordId,
                      phoneInfo,
                      appVersion,
                      healthCode, 
                      medTimepoint) %>%
        tibble::as_tibble()
}

#' function to map kinetic features based on 
#' mpower tools to mhealthtools
#' 
#' @param feature the feature sets
#' 
#' @return a mapped kinetic features
map_kinetic_features <- function(feature){
    print("mapping kinetic features")
    feature %>%
        dplyr::mutate(
            measurementType = case_when(
                str_detect(sensor, "accelerometer") ~ str_replace_all(
                    measurementType, KINETIC_MAPPING$acceleration), 
                str_detect(sensor, "gyroscope") ~ str_replace_all(
                    measurementType, KINETIC_MAPPING$gyroscope), 
                TRUE ~ measurementType))
}

summarise_features <- function(feature){
    feature %>%
        dplyr::summarise(across(
            .cols = everything(),
            .fns = list(md = ~median(.x, na.rm = TRUE), 
                        iqr = ~IQR(.x, na.rm = TRUE)),
            .names = "{.col}.{.fn}"))
}

#' function to aggregate features
#' for the tremor data
#' 
#' @param feature the feature sets
#' @param group the grouping for the aggregates
#' 
#' @param return an aggregated features of .fr, and .tm
group_features <- function(feature, group) {
    feature %>%
        dplyr::mutate(
            across(matches(".fr|.tm"), as.numeric)) %>%
        dplyr::group_by(across(all_of(c(group, 
                                        "fileColumnName", 
                                        "sensor", 
                                        "measurementType", 
                                        "axis")))) %>%
        dplyr::select(matches(".fr|.tm")) %>%
        tidyr::nest() %>%
        dplyr::bind_cols(future_map_dfr(.$data, summarise_features)) %>%
        dplyr::select(-data) %>%
        dplyr::ungroup()
}


#' function to melt feature of the tremor data
#' 
#' @param feature the feature sets
#' 
#' @return a long-to-wide dataframe of feature.agg_measurementType_sensor_axis
widen_features <- function(feature){
    feature %>%
        tidyr::pivot_wider(
            names_from = c(sensor, measurementType, axis),
            names_glue = "{.value}_{measurementType}_{sensor}_{axis}",
            values_from = matches(".md$|.iqr$"))
}

main <- function(){
    # get features
    feature <- OUTPUT_REF$feat_id %>% 
        syn$get(.) %>% 
        .$path %>% 
        fread() %>%
        dplyr::filter(is.na(error))
    
    # get demographics
    demo <- OUTPUT_REF$demo_id %>% 
        syn$get() %>% 
        .$path %>% 
        fread()
    
    # get identifiers
    identifier <- get_table(
        syn = syn,
        tbl_id = OUTPUT_REF$tbl_id)
        
    # get features aggregated with recordId
   feature %>%
       map_kinetic_features()  %>%
       group_features(group = c("recordId")) %>%
       widen_features() %>%
       dplyr::inner_join(identifier, by = c("recordId")) %>%
       dplyr::inner_join(demo, by = c("healthCode")) %>% 
       dplyr::select(recordId,
                  healthCode, 
                  diagnosis, 
                  age,
                  sex, 
                  fileColumnName,
                  medTimepoint,
                  matches(top_n_features(
                      syn = syn,
                      best_features_id = OUTPUT_REF$best_features_id))) %>%
   reticulated_save_to_synapse(
        syn = syn,
        synapseclient = synapseclient,
        data = .,
        output_filename = OUTPUT_REF$agg_record,
        parent= OUTPUT_REF$parent_id,
        used = OUTPUT_REF$feat_id,
        executed = GIT_URL, 
        name = "aggregate tremor by records")
}

main()
    