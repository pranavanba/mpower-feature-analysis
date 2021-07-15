library(tidyverse)
library(reticulate)
library(data.table)
library(ggpubr)
library(patchwork)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

TBL_REF <- list(
    mapping = "syn17015960",
    active_v2 = "syn25421316",
    passive = "syn25421320",
    mdsupdrs = "syn25050919",
    active_v2_medTimepoint = "syn25882410"
)


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
                                           .x[[metric_target]],
                                           method = "pearson")),
               tidied = map(test, broom::tidy)) %>%
        unnest(cols = tidied) %>% 
        dplyr::arrange(estimate) %>%
        dplyr::ungroup() %>%
        dplyr::mutate(n_user = length(unique(subset_updrs$healthCode))) %>% 
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

clean_features_and_join_updrs <- function(data, metrics_data){
    data %>%
        dplyr::select(healthCode, any_of(features)) %>%
        tidyr::drop_na(any_of(features)) %>%
        dplyr::inner_join(mapping, by = "healthCode") %>%
        dplyr::inner_join(metrics_data, by = c("externalId" = "guid"))
}

mapping <- syn$tableQuery(glue::glue(
"SELECT  distinct healthCode, externalId FROM {mapping} \
 where externalId is not null", mapping = TBL_REF$mapping))$asDataFrame()
agg_passive_data <- syn$get(TBL_REF$passive)$path %>% fread()
agg_walk_data <- syn$get(TBL_REF$active_v2)$path %>% fread()
agg_walk_data_med <- syn$get(TBL_REF$active_v2_medTimepoint)$path %>% fread()
ahpd_scores <- syn$get(TBL_REF$mdsupdrs)$path %>% fread()
baseline <- ahpd_scores %>% 
    dplyr::filter(visit == "Baseline") %>% 
    dplyr::group_by(guid) %>% 
    summarise_if(is.numeric, mean, na.rm = T) %>% 
    dplyr::ungroup()
multiple_visit <- ahpd_scores %>% 
    dplyr::group_by(guid) %>% 
    summarise_if(is.numeric, mean, na.rm = T) %>% 
    dplyr::ungroup()
features <- agg_walk_data %>% 
    dplyr::select(matches("^x|^AA_speed|^rotation"), 
                  -matches("^x_speed_of_gait",
                           "^y_speed_of_gait",
                           "^z_speed_of_gait"),
                  -ends_with("var")) %>% names(.)

baseline_walk_data <- agg_walk_data %>% 
    clean_features_and_join_updrs(metrics_data = baseline)
all_walk_data <- agg_walk_data %>% 
    clean_features_and_join_updrs(metrics_data = multiple_visit)
baseline_passive_data <- agg_passive_data %>% 
    clean_features_and_join_updrs(metrics_data = baseline)
all_passive_data <- agg_passive_data %>% 
    clean_features_and_join_updrs(metrics_data = multiple_visit)


choice_metrics <- c("UPDRS2", 
                    "UPDRS3", 
                    "UPDRSTOT", 
                    "UPDRSPRO")
run_sim(baseline_walk_data, "images/updrs_severity/baseline_active_walk_corr.png", 
        features = features, 
        metrics = choice_metrics)

run_sim(all_walk_data, "images/updrs_severity/multiple_visit_active_walk_corr.png", 
        features = features, 
        metrics = choice_metrics)

run_sim(baseline_passive_data, "images/updrs_severity/baseline_passive_walk_corr.png", 
        features = features, 
        metrics = choice_metrics)

run_sim(all_passive_data, 
        "images/updrs_severity/multiple_visit_passive_walk_corr.png", 
        features = features, 
        metrics = choice_metrics)


choice_metrics <- c("C_NP2WALK", "C_NP2FREZ")
    


