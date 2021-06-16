act <- matched_walk_data %>% dplyr::mutate(type = "active")
pas <- matched_passive_data %>% dplyr::mutate(type = "passive")

comb <- bind_rows(act,pas)

run_sim <- function(data, top_n, output_path){
    top_feat<- data %>% 
        get_two_sample_ttest(features = features,
                             group = "type") %>% 
        dplyr::slice(1:top_n)
    
    purrr::map(top_feat$feature, function(feature){
        data %>% 
            plot_boxplot(feature = feature,
                         group = "type")}) %>% 
        patchwork::wrap_plots(nrow = 3) +
        ggsave(output_path, 
               width = 20, 
               height = 20)
}


run_sim(comb, 10, "images/active_vs_passive/diff_active_vs_passive_boxplot.png")