library(tidyverse)
library(githubr)
library(data.table)
library(synapser)
synLogin()

BEST_FEATURES <- "syn17088603"
GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "data_summary","reduce_tremor_features.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)
SYN_ID_REF <- list(
    v1 = list(
        agg_record = "syn26010469",
        agg_hc = "syn26010470",
        tbl = "syn10676309",
        parent = "syn25782484"),
    v2 = list(
        agg_record = "syn26010471",
        agg_hc = "syn26010472",
        tbl = "syn12977322",
        parent = "syn25999253"
    )
)

#' function to get top n features
top_n_features <- function(n){
    synGet(BEST_FEATURES)$path %>% 
        fread() %>%
        dplyr::filter(assay == "rest") %>%
        dplyr::slice(1:n) %>% 
        .$Feature %>%
        glue::glue_collapse(sep = "|")
}

#' function to get table and parse 
#' medication columns accordingly
get_table <- function(tbl_id){
    synapser::synTableQuery(
        glue::glue("SELECT * FROM {id}", id = tbl_id))$asDataFrame() %>%
        dplyr::rename_with(
            .cols = matches("answers.medicationTiming"), 
            .fn = ~"medTimepoint") %>%
        dplyr::select(recordId, healthCode, medTimepoint)
}

purrr::map(c("v1", "v2"), function(activity){
    agg_record_entity <- SYN_ID_REF[[activity]]$agg_record %>% synGet()
    agg_hc_entity <- SYN_ID_REF[[activity]]$agg_hc %>% synGet()
    tbl_id <- SYN_ID_REF[[activity]]$tbl
    agg_record <- synapser::synGet(agg_record_entity$properties$id)$path %>% 
        fread() %>%
        dplyr::inner_join(get_table(tbl_id), on = "recordId") %>%
        dplyr::select(recordId,healthCode, 
                      diagnosis,age,sex, 
                      activityType,
                      medTimepoint,
                      matches(top_n_features(n = 30))) %>%
        tibble::as_tibble() %>%
        save_to_synapse(output_filename = glue::glue("reduced_features_{filename}",
                                                     filename = agg_record_entity$properties$name),
                        parent = SYN_ID_REF[[activity]]$parent,
                        used = c(agg_record_entity$properties$id, BEST_FEATURES))
    agg_hc <- synapser::synGet(agg_hc_entity$properties$id)$path %>% 
        fread() %>%
        dplyr::select(healthCode, 
                      diagnosis,
                      age,
                      sex, 
                      activityType,
                      matches(top_n_features(n = 30))) %>%
        tibble::as_tibble() %>%
        save_to_synapse(output_filename = glue::glue("reduced_features_{filename}",
                                                     filename = agg_hc_entity$properties$name),
                        parent = SYN_ID_REF[[activity]]$parent,
                        used = c(agg_hc_entity$properties$id, BEST_FEATURES))
    
})
