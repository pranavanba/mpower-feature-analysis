library(tidyverse)
library(data.table)
library(synapser)
library(ggplot2)
library(ggpubr)
source("R/utils/utils.R")

synapser::synLogin()

HC_MAPPING <- "syn26228966"
TAP_V1 <- "syn25767029"
TAP_V2 <- "syn26235452"

get_non_dominant_hand <- function(data){
    data %>%
        dplyr::group_by(healthCode, fileColumnName) %>%
        dplyr::summarise(avg_taps = mean(numberTaps, na.rm = T)) %>%
        dplyr::ungroup() %>%
        tidyr::pivot_wider(id_cols = healthCode, 
                           names_from = fileColumnName, 
                           values_from = avg_taps) %>%
        tidyr::drop_na(left_tapping.samples, right_tapping.samples) %>%
        dplyr::mutate(non_dominant_hand = case_when(
            is.na(left_tapping.samples) ~ "right",
            is.na(right_tapping.samples) ~ "left",
            left_tapping.samples > right_tapping.samples ~ "right",
            TRUE ~ "left"
        ))
}

summarize_users <- function(data){
    data %>%
        dplyr::group_by(user) %>%
        dplyr::summarise_if(is.numeric, list(
            "md" = median, "iqr" = IQR), na.rm = TRUE)
}

plot_paired_test <- function(data, feature){
    data %>%
        tidyr::pivot_wider(id_cols = "user", 
                           values_from = !!sym(feature), 
                           names_from = "version") %>% 
        tidyr::drop_na() %>%
        ggpaired(cond1 = "V1", 
                 cond2 = "V2", 
                 line.color = "gray",
                 ylab = feature,
                 fill = "condition",
                 palette = "jco",
                 font.label = list(size = 25, 
                                   color = "black")) +
        stat_compare_means(paired = TRUE, size = 5) +
        theme_minimal()
}

V1_table_mapping <- synTableQuery(
    "SELECT recordId, healthCode from syn10374665")$asDataFrame() %>%
    dplyr::select(recordId, healthCode)

V2_table_mapping <- synTableQuery(
    "SELECT recordId, healthCode from syn15673381")$asDataFrame() %>%
    dplyr::select(recordId, healthCode)

hc_mapping <- synGet(HC_MAPPING)$path %>% 
    fread() %>%
    tibble::as_tibble() %>%
    dplyr::mutate(user = glue::glue(
        "user{n}", n = dplyr::row_number()))

tap_features_v1 <- synGet(TAP_V1)$path %>% 
    fread() %>%
    tibble::as_tibble() %>%
    dplyr::inner_join(V1_table_mapping) %>%
    dplyr::select(recordId, healthCode, everything())
tap_features_v2 <- synGet(TAP_V2)$path %>% 
    fread() %>%
    tibble::as_tibble() %>%
    dplyr::inner_join(V2_table_mapping) %>%
    dplyr::select(recordId, healthCode, everything())


provenance <- synGetProvenance("syn25767029")
entity <- synGet("syn25767029")

tap_features_v1 %>%
    write_tsv("mhealthtools_tapping_features_mpower_v1_freeze.tsv")
f <- File("mhealthtools_tapping_features_mpower_v1_freeze.tsv", 
          parent = "syn26250286")
synStore(f, activity = provenance)


provenance <- synGetProvenance("syn26235452")
entity <- synGet("syn26235452")

tap_features_v2 %>%
    write_tsv(entity$properties$name)
f <- File(entity$properties$name, 
          parent = "syn26215075")
synStore(f, activity = provenance)


tap_features_v1_agg <- tap_features_v1 %>%
    summarize_users() %>%
    tidyr::drop_na() %>%
    dplyr::mutate(version = "V1")

tap_features_v2_agg <- tap_features_v2 %>%
    summarize_users() %>%
    tidyr::drop_na() %>%
    dplyr::mutate(version = "V2")

non_dominant_hand <- tap_features_v2 %>%
    get_non_dominant_hand() %>% 
    dplyr::inner_join(
        hc_mapping, 
        by = c("healthCode" = "healthCode_v2"))

tap_features_v1_agg_filtered <- tap_features_v1 %>%
    dplyr::mutate(fileColumnName = ifelse(fileColumnName == "tapping_left.json.TappingSamples" , "left", "right")) %>%
    dplyr::filter(fileColumnName == "right") %>%
    summarize_users() %>%
    tidyr::drop_na() %>%
    dplyr::mutate(version = "V1")

tap_features_v2_agg_filtered <- tap_features_v2 %>%
    dplyr::mutate(fileColumnName = ifelse(fileColumnName == "right_tapping.samples", "right", "left")) %>%
    dplyr::filter(fileColumnName == "right") %>%
    summarize_users() %>%
    tidyr::drop_na() %>%
    dplyr::mutate(version = "V2")
    
    

tap_features <- dplyr::bind_rows(tap_features_v1_agg, 
                                 tap_features_v2_agg) %>%
    group_by(user) %>%
    mutate(n = n_distinct(version))  %>%
    dplyr::ungroup() %>%
    dplyr::filter(n == 2)

features <- tap_features %>% 
    dplyr::select(matches("iqr|md")) %>%
    names(.)

names(features) <-  features


pval_test <- purrr::map_dfr(features, function(x){
    test <- tap_features %>%
        dplyr::select(user, version, all_of(x))  
    formula <- as.formula(glue::glue("{feature} ~ version", feature = x))
    w_test <- wilcox.test(formula, data = test, paired = TRUE, exact = FALSE) %>%
        .$p.value
    tibble(feature = x, p_val = w_test)}) %>%
    dplyr::arrange(p_val)


features_filtered <-  pval_test %>%
    dplyr::filter(!str_detect(feature, "buttonNone")) %>%
    dplyr::filter(p_val < 0.05) %>%
    .$feature
plots_data <- purrr::map(features_filtered, function(x){
    tap_features %>%
        plot_paired_test(x)}) %>%
    patchwork::wrap_plots() + 
    patchwork::plot_layout(guides = "collect")

ggsave("comparison.png",
       plots_data, 
       width = 60, 
       height = 50,
       units = "cm",
       limitsize= FALSE)




tap_features <- dplyr::bind_rows(tap_features_v1_agg_filtered, 
                                 tap_features_v2_agg_filtered) %>%
    group_by(user) %>%
    mutate(n = n_distinct(version))  %>%
    dplyr::ungroup() %>%
    dplyr::filter(n == 2)

features <- tap_features %>% 
    dplyr::select(matches("iqr|md")) %>%
    names(.)

names(features) <-  features

pval_test <- purrr::map_dfr(features, function(x){
    test <- tap_features %>%
        dplyr::select(user, version, all_of(x))  
    formula <- as.formula(glue::glue("{feature} ~ version", feature = x))
    w_test <- wilcox.test(formula, data = test, paired = TRUE, exact = FALSE) %>%
        .$p.value
    tibble(feature = x, p_val = w_test)}) %>%
    dplyr::arrange(p_val)


features_filtered <-  pval_test %>%
    dplyr::filter(!str_detect(feature, "buttonNone")) %>%
    dplyr::slice(1:20) %>%
    .$feature
plots_data <- purrr::map(features_filtered, function(x){
    tap_features %>%
        plot_paired_test(x)}) %>%
    patchwork::wrap_plots(ncol = 4, nrow = 5) + 
    patchwork::plot_layout(guides = "collect")



