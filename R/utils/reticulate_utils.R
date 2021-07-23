#' function to check whether medication timepoint exist
parse_medTimepoint <- function(data){
  if("answers.medicationTiming" %in% names(data)){
    data %>% 
      dplyr::rowwise() %>%
      dplyr::mutate(medTimepoint = glue::glue_collapse(answers.medicationTiming, ", ")) %>%
      dplyr::ungroup()
  }else if("medTimepoint" %in% names(data)){
    data %>% 
      dplyr::mutate(medTimepoint = unlist(medTimepoint))
  }else{
    data %>% 
      dplyr::mutate(medTimepoint = NA)
  }
}

#' function to parse phone information
parse_phoneInfo <- function(data){
  data %>%
    dplyr::mutate(
      operatingSystem = ifelse(str_detect(phoneInfo, "iOS|iPhone"), "iOS", "Android"))
}

generate_query_clause <- function(id, 
                                  cols = NULL, 
                                  nrow = NULL, 
                                  cond = NULL){
  if(!is.null(cols)){
    cols <-  stringr::str_c(cols, collapse = ",")
  }else{
    cols <- "*"
  }
  query_string <- glue::glue("select {cols} from {id}")
  if(!is.null(cond)){
    query_string <- glue::glue(query_string, " where ", cond)
  }
  if(!is.null(nrow)){
    query_string <- glue::glue(query_string, " limit ", nrow)
  }
  return(query_string)
}

get_table <- function(syn, synapse_tbl, 
                      download_file_columns = NULL, ...){
  
  # get table entity
  entity <- generate_query_clause(id = synapse_tbl, ...) %>% 
    syn$tableQuery(.)
  
  print( generate_query_clause(id = synapse_tbl))
  
  # check if download file columns
  if(is.null(download_file_columns)){
    result <- entity$asDataFrame()
  }else{
    # shape table
    table <- entity$asDataFrame() %>%
      tibble::as_tibble(.) %>%
      tidyr::pivot_longer(cols = all_of(download_file_columns), 
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
      columns = download_file_columns) %>%
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

save_to_synapse <- function(syn,
                            synapseclient,
                            data, 
                            output_filename, 
                            parent,
                            ...){
  data %>% 
    readr::write_tsv(output_filename)
  file <- synapseclient$File(output_filename, parent = parent)
  activity <- synapseclient$Activity(...)
  store_to_synapse <- syn$store(file, activity = activity)
  unlink(output_filename)
}

