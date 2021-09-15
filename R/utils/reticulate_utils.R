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

reticulated_save_to_synapse <- function(syn,
                            synapseclient,
                            data, 
                            output_filename, 
                            parent_id,
                            ...){
  data %>% 
    readr::write_tsv(output_filename)
  file <- synapseclient$File(output_filename, parent = parent_id)
  activity <- synapseclient$Activity(...)
  store_to_synapse <- syn$store(file, activity = activity)
  unlink(output_filename)
}

