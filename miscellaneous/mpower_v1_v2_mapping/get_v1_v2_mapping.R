library(tidyverse)
library(data.table)
library(synapser)
library(bridgeclient)
source("R/utils/utils.R")

synapser::synLogin()

V1_V2_link <- "syn26199014"

data <- synGet(V1_V2_link)$path %>% 
    fread() %>%
    dplyr::select(healthCode_v2 = healthCode, 
                  email = mPower1Email)
unique_email <- data %>%
    .$email %>%
    unique()

# login
bridgeclient::bridge_login(study = "parkinson")
email_mapping <- purrr::map(unique_email, function(email){
    tryCatch({
        # get email to user id mapping
        user_id <- bridgeclient::search_participants(
            email = email) %>%
            .$items %>%
            .[[1]] %>% 
            .$id
        # get healthcode
        healthCode <- bridgeclient::get_participant(
            user_id = user_id) %>%
            .$healthCode
        tibble::tibble(email = email, 
                       healthCode_v1 = healthCode)  
    }, error = function(e){
        print(e)
        tibble::tibble(email = email, 
                       healthCode_v1 = NA)})}) %>% 
    purrr::reduce(dplyr::bind_rows)

healthCode_mapping <- email_mapping %>%
    tidyr::drop_na() %>%
    dplyr::inner_join(data) %>%
    distinct(healthCode_v1, healthCode_v2) %>%
    save_to_synapse(
        output_filename = "v1_v2_hc_mapping.tsv",
        parent = "syn26199002",
        name = "get email pairing from bridge",
        used = V1_V2_link
    )





