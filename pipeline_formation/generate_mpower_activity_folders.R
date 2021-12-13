library(synapser)
library(tidyverse)
library(data.table)
source("utils/helper_utils.R")


PROJECT_ID <- config::get("project_id")
FILE_VIEW_NAME <- config::get("file_view_name")
MAPPING <- list(
    "resting" = "Rest",
    "tapping" = "Tapping", 
    "tremor" = "Tremor",
    "walk30secs" = "Walk 30 Seconds")
TARGET_ANNOTATIONS <- c("features-extracted", 
                        "features-processed")

#' Function for creating file view with annotations
#' 
#' @param parent_id project/parent id to populate with mPower features
#' 
#' @return Folders of mPower pipeline
create_activity_template <- function(parent_id, mapping, annotations){
    ## create basic skeleton
    purrr::map(names(mapping),
               function(subtype){
                   annotations$task <- tolower(subtype)
                   folder <- Folder(mapping[[subtype]], 
                                    parent = parent_id)
                   folder <- synStore(folder)
                   annotations <- synSetAnnotations(
                       folder$properties$id,
                       annotations)
                   return(folder$properties$id)})
}

get_folder_target_id <- function(target, file_view_id){
    query_clause <- glue::glue(
        "SELECT * FROM {file_view_id} where pipelineStep = '{target}'",
        file_view_id = file_view_id,
        target = target)
    id <- synTableQuery(query_clause)$asDataFrame() %>%
        tibble::as_tibble() %>%
        dplyr::filter(type == "folder" & is.na(task)) %>%
        .$id
    return(id)
}

main <- function(){
    file_view_id <- synFindEntityId(FILE_VIEW_NAME, PROJECT_ID)
    
    purrr::map(TARGET_ANNOTATIONS, function(annot_target){
        folder_id <- get_folder_target_id(annot_target, 
                                          file_view_id)
        create_activity_template(
            parent_id = folder_id, 
            mapping = MAPPING, 
            annotations = list(pipelineStep = annot_target))
    })
}

main()