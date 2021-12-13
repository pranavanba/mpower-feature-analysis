library(synapser)
library(tidyverse)
library(data.table)
source("utils/curation_utils.R")

synapser::synLogin()

PROJECT_ID <- config::get("project_id")
FILE_VIEW_NAME <- config::get("file_view_name")
GIT_URL <- get_github_url(
    git_token = config::get("git")$token_path,
    git_repo = config::get("git")$repo_path,
    script_path = "create_pipeline_directory/generate_folders_org.R"
)

#' Function for creating file view with annotations
#' 
#' @param parent_id project/parent id to populate with mPower features
#' 
#' @return Folders of mPower pipeline
create_pipeline_template <- function(parent_id){
    ## create basic skeleton
    folder_mapping <- list(
        "analysis-committed" = "Analysis - Committed",
        "analysis-sandbox" = "Analysis - Sandbox", 
        "features-extracted" = "Features - Extracted",
        "features-processed" = "Features - Processed",
        "miscellaneous" = "Miscellaneous")
    purrr::map(names(folder_mapping),
               function(subtype){
                   folder <- Folder(folder_mapping[[subtype]], 
                                    parent = parent_id)
                   folder$annotations <- list(
                       pipelineStep = tolower(subtype))
                   folder <- synStore(folder)
                   return(folder$properties$id)})
}

#' Function for creating file view with annotations
#' 
#' @param file_view_name name of desired file view
#' @param project_id project id to populate with mPower features
#' 
#' @return File View schema to query mPower Features
create_file_view <- function(file_view_name, project_id){
    EntityViewSchema(name= file_view_name,
                     columns = c(
                         Column(name = "task",
                                columnType = "STRING",
                                maximumSize = 10),
                         Column(name = "analysisType",
                                columnType = "STRING",
                                maximumSize = 30),
                         Column(name = "analysisSubtype",
                                columnType = "STRING"),
                         Column(name = "pipelineStep",
                                columnType = "STRING",
                                maximumSize = 30)),
                     parent = project_id,
                     add_default_columns=F,
                     scopes = project_id,
                     includeEntityTypes=c(EntityViewType$FILE, 
                                          EntityViewType$FOLDER)) %>%
        synapser::synStore()
}


main <- function(){
    create_pipeline_template(PROJECT_ID)
    create_file_view(FILE_VIEW_NAME, PROJECT_ID)
}

main()