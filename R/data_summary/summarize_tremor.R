library(tidyverse)
library(githubr)
library(data.table)
library(furrr)
library(future)
source("R/utils/reticulate_utils.R")

#' used synapse python client -> better parallelization
future::plan(multisession)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$Synapse()
syn$login()
syn$table_query_timeout <- 9999999

GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "data_summary","summarize_tremor.R")
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
    tremor_v1 = list(
        agg_hc = "mhealthtools_tremor_freeze_agg_healthcodes.tsv",
        agg_record = "mhealthtools_tremor_freeze_agg_recordId.tsv",
        feat_id = "syn26001251",
        tbl_id = "syn10676309",
        demo_id = "syn25782458",
        parent_id = "syn25782484",
        funs = "summarize_tremor"
    ),
    tremor_v2 = list(
        agg_hc = "mhealthtools_tremor_v2_agg_healthcodes.tsv",
        agg_record = "mhealthtools_tremor_v2_agg_recordId.tsv",
        feat_id = "syn25701650",
        tbl_id = "syn12977322",
        demo_id = "syn25693310",
        parent_id = "syn25999253",
        funs = "summarize_tremor"
    )
)



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
                                        "activityType", 
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
    # map each activity
    purrr::walk(names(OUTPUT_REF), function(activity){
        
        # get features
        feature <- OUTPUT_REF[[activity]]$feat_id %>% 
            syn$get() %>% 
            .$path %>% 
            fread() %>%
            dplyr::filter(is.na(error))
        
        # get demographics
        demo <- OUTPUT_REF[[activity]]$demo_id %>% 
            syn$get() %>% 
            .$path %>% 
            fread()
        
        # get identifiers
        identifier <- syn$tableQuery(
            glue::glue(
                "select recordId, healthCode from {tbl_id}", 
                tbl_id = OUTPUT_REF[[activity]]$tbl_id))$asDataFrame()
        
        # get features aggregated with recordId
       feature %>%
            map_kinetic_features()  %>%
            group_features(group = c("recordId")) %>%
            widen_features() %>%
            dplyr::inner_join(identifier, by = c("recordId")) %>%
            dplyr::inner_join(demo, by = c("healthCode")) %>%
            save_to_synapse(
                syn = syn,
                synapseclient = synapseclient,
                data = .,
                output_filename = OUTPUT_REF[[activity]]$agg_record,
                parent= OUTPUT_REF[[activity]]$parent_id,
                used = OUTPUT_REF[[activity]]$feat_id,
                executed = GIT_URL, 
                name = "aggregate tremor by records")
       
       # get features aggregated with healthcode
       feature %>%
            dplyr::inner_join(identifier, by = c("recordId")) %>%
            dplyr::group_by(healthCode, activityType) %>%
            dplyr::mutate(nrecords = n_distinct(recordId)) %>%
            map_kinetic_features() %>%
            group_features(group = c("healthCode", "nrecords")) %>%
            widen_features() %>%
            dplyr::inner_join(demo, by = c("healthCode")) %>%
            save_to_synapse(
                syn = syn,
                synapseclient = synapseclient,
                data = .,
                output_filename = OUTPUT_REF[[activity]]$agg_hc,
                parent= OUTPUT_REF[[activity]]$parent_id,
                used = OUTPUT_REF[[activity]]$feat_id,
                executed = GIT_URL,
                name = "aggregate tremor by healthcodes")
    })
}

main()
    