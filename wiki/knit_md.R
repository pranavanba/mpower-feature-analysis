library(knit2synapse)
library(synapser)
library(tidyverse)
synapser::synLogin()

#' function to clean wiki generation cache from knit2synapse
clean_cache <- function(){
    list.files("wiki") %>% 
        purrr::walk(function(file){
            if(stringr::str_detect(file, "_cache$")){
                unlink(file.path("wiki", file), recursive = T)
            }
        }) 
}

# get project id
template_path <- file.path("synapseformation/manuscript.yaml")
project_id <- synFindEntityId(
    yaml::read_yaml(template_path)[[1]]$name)

# knit wiki page
knit2synapse::createAndKnitToFolderEntity(
    file = "wiki/generate_feature_extraction_wiki.Rmd",
    parentId = project_id, 
    wikiName = "Feature Extraction",
    folderName = "Features - Extracted")


# knit wiki page
knit2synapse::createAndKnitToFolderEntity(
    file = "wiki/generate_feature_processed_wiki.Rmd",
    parentId = project_id, 
    wikiName = "Feature Processed",
    folderName = "Features - Processed")



# clean all cache
clean_cache()