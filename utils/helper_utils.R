###############################################################
#' Collection of script to streamline
#' pipeline workflow
#' 
#' Note: using reticulate function
#' to adjust with reticulated synapse 
#' (less error in parallelization and python package usage)
#' 
#' @author: aryton.tediarjo@sagebase.org
####################################################################

#' Function (reticulated) to get synapse table in reticulate
#' into tidy-format (pivot based on filecolumns if exist)
#' @param synapse_tbl tbl id in synapse
#' @param file_columns file_columns to download
reticulated_get_table <- function(syn, 
                                  tbl_id, 
                                  file_columns = NULL,
                                  query_params = NULL){
  
  # get table entity
  if(is.null(query_params)){
    entity <- glue::glue("select * from {tbl_id}")
  }else{
    entity <- glue::glue("select * from {tbl_id} {query_params}") 
  }
  entity <- entity %>% syn$tableQuery(.)
  
  # check if download file columns
  if(is.null(file_columns)){
    result <- entity$asDataFrame()
  }else{
    # shape table
    table <- entity$asDataFrame() %>%
      tibble::as_tibble(.) %>%
      tidyr::pivot_longer(cols = all_of(file_columns), 
                          names_to = "fileColumnName", 
                          values_to = "fileHandleId") %>%
      dplyr::mutate(across(everything(), unlist)) %>%
      dplyr::filter(!is.na(fileHandleId)) %>%
      dplyr::group_by(recordId, fileColumnName) %>% 
      dplyr::summarise_all(last) %>% 
      dplyr::ungroup() %>%
      dplyr::mutate(
        createdOn = as.POSIXct(createdOn/1000, 
                               origin="1970-01-01"),
        fileHandleId = as.character(fileHandleId))
    # download all table columns
    result <- syn$downloadTableColumns(
      table = entity, 
      columns = file_columns) %>%
      tibble::enframe(.) %>%
      tidyr::unnest(value) %>%
      dplyr::select(
        fileHandleId = name, 
        filePath = value) %>%
      dplyr::mutate(filePath = unlist(filePath)) %>%
      dplyr::right_join(table, by = c("fileHandleId"))
  }
  return(result)
}

#' Function (reticulated) to save to synapse
#' 
#' @param data dataframe
#' @param output_filename desired output filename
#' @param parent parent synapse id
#' @param annotations synapse annotations (list)
#' @param ... addditional params for provenance
reticulated_save_to_synapse <- function(syn,
                            synapseclient,
                            data, 
                            output_filename, 
                            parent_id,
                            annotations = NULL,
                            ...){
  data %>% 
    readr::write_tsv(output_filename)
  file <- synapseclient$File(
    output_filename, 
    parent = parent_id,
    annotations = annotations)
  activity <- synapseclient$Activity(...)
  store_to_synapse <- syn$store(file, activity = activity)
  unlink(output_filename)
}


#' function to get synapse table in reticulate
#' into tidy-format (pivot based on filecolumns if exist)
#' @param synapse_tbl tbl id in synapse
#' @param file_columns file_columns to download
get_table <- function(synapse_tbl, 
                      file_columns = NULL,
                      query_params = NULL){
  # get table entity
  if(is.null(query_params)){
    entity <- synTableQuery(glue::glue("SELECT * FROM {synapse_tbl}"))
  }else{
    entity <- synTableQuery(glue::glue("SELECT * FROM {synapse_tbl} {query_params}"))
  }
  
  # shape table
  table <- entity$asDataFrame() %>%
    tibble::as_tibble(.)
  
  # download all table columns
  if(!is.null(file_columns)){
    table <-  table %>%
      tidyr::pivot_longer(
        cols = all_of(file_columns), 
        names_to = "fileColumnName", 
        values_to = "fileHandleId") %>%
      dplyr::mutate(across(everything(), unlist)) %>%
      dplyr::mutate(fileHandleId = as.character(fileHandleId)) %>%
      dplyr::filter(!is.na(fileHandleId)) %>%
      dplyr::group_by(recordId, fileColumnName) %>% 
      dplyr::summarise_all(last) %>% 
      dplyr::ungroup()
    result <- synDownloadTableColumns(
      table = entity, 
      columns = file_columns) %>%
      tibble::enframe(.) %>%
      tidyr::unnest(value) %>%
      dplyr::select(
        fileHandleId = name, 
        filePath = value) %>%
      dplyr::mutate(filePath = unlist(filePath)) %>%
      dplyr::right_join(table, by = c("fileHandleId"))
  }else{
    result <- table
  }
  return(result)
}

#' Utility function to get github url
#' 
#' @param git_token_path path to git token
#' @param git_repo github repo
#' @param script_path path to script
#' @param ... additional param for githubr
#' 
#' @return git url
get_github_url <- function(git_token_path, 
                           git_repo,
                           script_path,
                           ...){
  gh::gh("/repos/:owner_repo/commits?path=:path_to_file", 
         owner_repo = git_repo, 
         path_to_file = script_path,
         .token = readLines(git_token_path))[[1]]$html_url %>% 
    stringr::str_replace("commit", "blob") %>% 
    file.path(script_path)
}

#' Function to save to synapse
#' 
#' @param data dataframe
#' @param output_filename desired output filename
#' @param parent parent synapse id
#' @param annotations synapse annotations (list)
#' @param ... addditional params for provenance
save_to_synapse <- function(data, 
                            output_filename, 
                            parent,
                            annotations = NULL,
                            ...){
  data %>% 
    readr::write_tsv(output_filename)
  file <- File(output_filename, 
               parent =  parent,
               annotations = annotations)
  activity <- Activity(...)
  synStore(file, activity = activity)
  unlink(file$path)
}

#' Function to get annotation map of features
#' 
#' @param refs takes in reference of synapseIDS
#' @return tibble of each synapseID and key annotationss
get_annotation_mapper <- function(refs){
  refs %>% 
    purrr::list_modify("parent_id" = NULL) %>% 
    purrr::map_dfr(function(x){
      entity <- synGet(x)
      id <- entity$properties$id
      annotations <- synGetAnnotations(id) %>% 
        unlist(recursive = F)
      analysisType = annotations$analysisType
      filter = ifelse(!is.null(annotations$filter), 
                      annotations$filter, NA_character_)
      tool = ifelse(!is.null(annotations$tool), 
                    annotations$tool,NA_character_)
      tibble::tibble(
        id = id,
        analysisType = analysisType,
        filter = filter,
        tool = tool)
    })
}

#' Function (reticulated) to get annotation map of features
#' 
#' @param refs takes in reference of synapseIDS
#' @return tibble of each synapseID and key annotationss
reticulated_get_annotation_mapper <- function(refs, syn){
  refs %>% 
    purrr::list_modify("parent_id" = NULL) %>% 
    purrr::map_dfr(function(x){
      entity <- syn$get(x)
      id <- entity$properties$id
      annotations <- syn$getAnnotations(id) %>% 
        unlist(recursive = F)
      analysisType = tryCatch({annotations$analysisType}, 
                              error = function(e){NA_character_})
      filter = tryCatch({annotations$filter}, 
                        error = function(e){NA_character_})
      tool = tryCatch({annotations$tool}, 
                      error = function(e){NA_character_})
      tibble::tibble(
        id = id,
        analysisType = analysisType,
        filter = filter,
        tool = tool)
    })
}

#' Utility feature extraction function to
#' map based on file column name and recordId
#' (inherited from get_table())
#' 
#' @param data
#' @return dataframe/tibble tapping features
map_feature_extraction <- function(data, 
                                   file_parser, 
                                   feature_funs,
                                   ...){
  features <- furrr::future_pmap_dfr(
    list(recordId = data$recordId,
         fileColumnName = data$fileColumnName,
         filePath = data$filePath),
    file_parser = file_parser,
    feature_funs = feature_funs,
    function(recordId,
             fileColumnName,
             filePath,
             file_parser,
             feature_funs){
      file_parser <- partial(file_parser)
      feature_funs <- partial(feature_funs, ...)
      filePath %>%
        file_parser() %>%
        feature_funs() %>%
        dplyr::mutate(
          recordId = recordId,
          fileColumnName = fileColumnName) %>%
        dplyr::select(recordId,
                      fileColumnName,
                      everything())})
  data %>%
    dplyr::select(
      all_of(c("recordId",
               "fileColumnName"))) %>%
    dplyr::left_join(
      features, by = c("recordId", "fileColumnName"))
}


