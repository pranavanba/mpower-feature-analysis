library(tidyverse)
library(reticulate)
library(data.table)
library(ggpubr)
library(patchwork)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

clean_data <- function(data){
    data %>% 
        dplyr::inner_join(demo, by = c("healthCode")) %>% 
        dplyr::select(diagnosis, age, sex, healthCode, all_of(features)) %>%
        tidyr::drop_na() %>%
        dplyr::filter(diagnosis == "control" | diagnosis == "parkinsons") %>%
        dplyr::mutate(diagnosis = factor(
            diagnosis, 
            levels = c("control", "parkinsons")))
}

get_two_sample_ttest <- function(feature_data){
    feature_data %>%
        dplyr::select(healthCode, diagnosis, all_of(features)) %>% 
        tidyr::pivot_longer(cols = all_of(features),
                            names_to = "feature") %>%
        dplyr::group_by(feature) %>%
        tidyr::nest() %>%
        dplyr::mutate(test = map(data, ~ t.test(value ~ diagnosis, data = .x)),
                      tidied = map(test, broom::tidy),
                      n = map(data, ~ length(unique(.x$healthCode)))) %>%
        unnest(cols = c(tidied,n)) %>% 
        dplyr::select(n_user = n,
                      feature, 
                      estimate, p.value, 
                      method, 
                      starts_with("conf")) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(p.value)
}

plot_boxplot <- function(data, feature, group){
    data %>% 
        ggplot(aes_string(y = feature, 
                          x = group,
                          fill = group)) +
        geom_boxplot(alpha = 0.5) + 
        geom_jitter(
            alpha = 0.5,
            position=position_jitter(width=.1, height=0)) +
        ggpubr::stat_compare_means() + 
        scale_fill_brewer(palette = "Dark2") +
        theme_minimal()
}

run_sim <- function(data, top_n, output_path){
    top_feat<- data %>% 
        get_two_sample_ttest() %>% 
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

demo <- syn$get("syn25421202")$path %>% fread
agg_walk_data <- syn$get("syn25421316")$path %>% 
    fread() %>% 
    clean_data()
agg_passive_data <- syn$get("syn25421320")$path %>% 
    fread() %>%
    clean_data()
features <- agg_walk_data %>% 
    dplyr::select(matches("^x|^AA_speed|^rotation"), 
                  -any_of("x_speed_of_gait_md")) %>% 
    names(.)

run_sim(agg_walk_data, 10, 
        "images/unmatched_active_walk_pd_vs_controls.png")
run_sim(agg_passive_data, 10, 
        "images/unmatched_passive_walk_pd_vs_controls.png")

    

