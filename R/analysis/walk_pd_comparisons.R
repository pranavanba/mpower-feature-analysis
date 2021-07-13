library(tidyverse)
library(reticulate)
library(data.table)
library(ggpubr)
library(patchwork)
source("R/utils/analysis_utils.R")

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

TBL_REF <- list(
    active_v2 = "syn25421316",
    passive = "syn25421320",
    demo = "syn25421202",
    active_v2_medTimepoint = "syn25882410"
)


clean_data <- function(data, 
                       metadata = c("healthCode", "diagnosis",
                                    "nrecords", "age", "sex")){
    data %>% 
        dplyr::inner_join(demo, by = c("healthCode")) %>% 
        dplyr::select(all_of(metadata), all_of(features)) %>%
        tidyr::drop_na() %>%
        dplyr::filter(diagnosis == "control" | diagnosis == "parkinsons") %>%
        dplyr::mutate(diagnosis = factor(
            diagnosis, 
            levels = c("control", "parkinsons")))
}


run_sim <- function(data, top_n, output_path){
    top_feat<- data %>% 
        get_two_sample_ttest(features = features,
                             group = "diagnosis") %>% 
        dplyr::slice(1:top_n)
    
    purrr::map(top_feat$feature, function(feature){
        data %>% 
            plot_boxplot(feature = feature,
                         group = "diagnosis")}) %>% 
        patchwork::wrap_plots(nrow = 3) +
        ggsave(output_path, 
               width = 20, 
               height = 20)
}

demo <- syn$get(TBL_REF$demo)$path %>% fread
agg_walk_data <- syn$get(TBL_REF$active_v2)$path %>% 
    fread() %>% 
    clean_data()
matched_active_hc <- agg_walk_data %>% 
    match_healthcodes(records_thresh = 0, 
                      plot = TRUE) %>% 
    .$data
matched_walk_data <- agg_walk_data %>% 
    dplyr::filter(healthCode %in% matched_active_hc$healthCode)

agg_passive_data <- syn$get(TBL_REF$passive)$path %>% 
    fread() %>%
    clean_data()
matched_passive_hc <- agg_passive_data %>% 
    match_healthcodes(records_thresh = 0, 
                      plot = TRUE) %>% 
    .$data
matched_passive_data <- agg_passive_data %>% 
    dplyr::filter(healthCode %in% matched_passive_hc$healthCode)
features <- agg_walk_data %>% 
    dplyr::select(matches("^x|^AA_speed|^rotation"), 
                  -matches("^x_speed_of_gait",
                           "^y_speed_of_gait",
                           "^z_speed_of_gait")) %>% 
    names(.)

plot_active_unmatched <- run_sim(agg_walk_data, 10, 
        "images/cases_vs_controls/unmatched_active_walk_pd_vs_controls.png")
plot_active_matched <-run_sim(matched_walk_data, 10, 
        "images/cases_vs_controls/matched_active_walk_pd_vs_controls.png")

plot_passive_unmatched <-run_sim(agg_passive_data, 10, 
        "images/cases_vs_controls/unmatched_passive_walk_pd_vs_controls.png")
plot_passive_matched <-run_sim(matched_passive_data, 10, 
        "images/cases_vs_controls/matched_passive_walk_pd_vs_controls.png")

    

