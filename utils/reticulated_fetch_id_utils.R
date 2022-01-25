get_file_view_ref <- function(syn){
    project_id <- syn$findEntityId(
        yaml::read_yaml("synapseformation/manuscript.yaml")[[1]]$name)
    file_view_id <- syn$findEntityId(
        "mPower2.0 - File View", project_id)
    return(file_view_id)
}

get_feature_extraction_ids <- function(syn){
    file_view_id <- get_file_view_ref(syn)
    ref_list <- list()
    data <- syn$tableQuery(
        glue::glue("SELECT * FROM {file_view_id}", 
                   file_view_id = file_view_id))$asDataFrame() %>%
        tibble::as_tibble()
    ref_list$parent_id <- data %>%
        dplyr::filter(type == "folder",
                      name == "Features - Extracted") %>% .$id
    return(ref_list)
}