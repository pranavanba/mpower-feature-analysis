library(tidyverse)
library(reticulate)
library(data.table)
library(ggpubr)
library(patchwork)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

get_corr <- function(data, features, metric_target){
    subset_updrs <- data %>% 
        dplyr::select(healthCode, 
                      any_of(features), 
                      any_of(metric_target))
    subset_updrs %>% 
        pivot_longer(cols = any_of(features),
                     names_to = "feature") %>% 
        dplyr::group_by(feature) %>%
        tidyr::nest() %>% 
        mutate(test = map(data, ~ cor.test(.x$value, 
                                           .x[[metric_target]])),
               tidied = map(test, broom::tidy)) %>%
        unnest(cols = tidied) %>% 
        dplyr::arrange(estimate) %>%
        dplyr::mutate(n_user = length(unique(data$healthCode))) %>% 
        dplyr::select(feature, n_user, estimate, p.value)
}

run_sim <- function(data, output_path, features, metrics, top_n = 3){
    ## baseline
    top_feat <- purrr::map_dfr(metrics, function(metric){
        data %>% 
            get_corr(features, metric) %>% 
            dplyr::ungroup() %>%
            dplyr::slice(1:top_n) %>%
            dplyr::mutate(metric = metric)
    })
    
    purrr::map2(top_feat$feature, top_feat$metric, 
                function(feature, metric){
                    data %>% 
                        ggplot(aes_string(y = feature, x = metric)) +
                        geom_smooth(method = lm, 
                                    color = "red") + 
                        geom_point(alpha = 0.7) +
                        ggpubr::stat_cor() +
                        theme_minimal()}) %>% 
        patchwork::wrap_plots(ncol = top_n) +
        ggsave(output_path, width = 15, height = 10)
}

agg_passive_data <- syn$get("syn25421320")$path %>% fread()
agg_walk_data <- syn$get("syn25421316")$path %>% fread()
ahpd_scores <- syn$get("syn25050919")$path %>% fread()
updrs_metrics <- ahpd_scores %>% 
    dplyr::group_by(guid) %>% 
    summarise_if(is.numeric, 
                 mean, 
                 na.rm = T)

features <- agg_walk_data %>% 
    dplyr::select(matches("^x|^AA_speed|^rotation"), 
                  ends_with("md"),
                  ends_with("iqr"),
                  -matches("^x_speed_of_gait",
                           "^y_speed_of_gait",
                           "^z_speed_of_gait")) %>%
    names(.)

baseline_walk_data <- agg_walk_data %>%
    dplyr::inner_join(mapping, by = "healthCode") %>%
    dplyr::inner_join(baseline, by = c("externalId" = "guid")) %>%
    dplyr::group_by(healthCode) %>% 
    dplyr::summarise_if(is.numeric, mean) %>% 
    tidyr::drop_na(all_of(features))

all_walk_data <- agg_walk_data %>%
    dplyr::inner_join(mapping, by = "healthCode") %>%
    dplyr::inner_join(updrs_metrics, by = c("externalId" = "guid")) %>%
    dplyr::group_by(healthCode) %>% 
    dplyr::summarise_if(is.numeric, mean) %>% 
    tidyr::drop_na(all_of(features))

baseline_passive_walk_data <- passive_walk_data %>%
    dplyr::inner_join(mapping, by = "healthCode") %>%
    dplyr::inner_join(baseline, by = c("externalId" = "guid")) %>%
    dplyr::group_by(healthCode) %>% 
    dplyr::summarise_if(is.numeric, mean) %>% 
    tidyr::drop_na(any_of(features))

all_passive_walk_data <- passive_walk_data %>%
    dplyr::inner_join(mapping, by = "healthCode") %>%
    dplyr::inner_join(updrs_metrics, by = c("externalId" = "guid")) %>%
    dplyr::group_by(healthCode) %>% 
    dplyr::summarise_if(is.numeric, mean) %>% 
    tidyr::drop_na(any_of(features))


run_sim(baseline_walk_data, "images/baseline_active_walk_corr.png", 
        features = features, 
        metrics = c("UPDRS2", "UPDRS3", "UPDRSTOT", "UPDRSPRO"))

run_sim(all_walk_data, "images/multiple_visit_active_walk_corr.png", 
        features = features, 
        metrics = c("UPDRS2", "UPDRS3", "UPDRSTOT", "UPDRSPRO"))

run_sim(baseline_passive_walk_data, "images/baseline_passive_walk_corr.png", 
        features = features, 
        metrics = c("UPDRS2", "UPDRS3", "UPDRSTOT", "UPDRSPRO"))

run_sim(all_passive_walk_data, "images/multiple_visit_passive_walk_corr.png", 
        features = features, 
        metrics = c("UPDRS2", "UPDRS3", "UPDRSTOT", "UPDRSPRO"))



