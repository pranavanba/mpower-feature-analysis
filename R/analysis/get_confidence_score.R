library(tidyverse)
library(githubr)
library(reticulate)
library(data.table)
library(tidymodels)
source("R/utils.R")
set.seed(1000)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()


WALK_TBL <- "syn25705738"
ROT_TBL <- "syn25705739"
TAP_TBL <- "syn25705827"

BACKGROUND_TABLE <- "syn17008627"
TABLE_MAPPING <- "syn17015960"
MOCA_TOTAL_COLUMN <- "moca_total"
HOEHN_AND_YAHR_COLUMN <- "hoehnyahrstagescor"
CGI_SEVERITY_COLUMN <- "severity_thistime"
PGI_SEVERITY_COLUMN <- "pgi_ill"
S_AND_W_COLUMN <- "tscaleschengdallivscl"
PDQ8_COLUMNS <- list("public", "dress", "depressed", "relationships",
                     "concentration", "communicate", "cramps", "embarrassed")
MDS_UPDRS_SCORES <- "syn25050919"

mapping <- syn$tableQuery(glue::glue("SELECT  distinct healthCode, externalId FROM {TABLE_MAPPING} where externalId is not null"))$asDataFrame()
ahpd_scores <- syn$get(MDS_UPDRS_SCORES)$path %>% fread()

baseline <- ahpd_scores %>% 
    dplyr::filter(visit == "Baseline")
walk_data <- syn$get(WALK_TBL)$path %>% fread()
rot_data <- syn$get(ROT_TBL)$path %>% fread()
tap_data <- syn$get(TAP_TBL)$path %>% fread()
agg_walk_data <- walk_data %>% 
    dplyr::inner_join(rot_data) %>% 
    drop_na()
agg_tap_data <- tap_data %>% drop_na()
walk_model <- readRDS("model/walk_model_mpowerV1_freeze.rds")
tap_model <- readRDS("model/tap_model_mpowerV1_freeze.rds")

walk_prediction <- predict(walk_model, 
        select(agg_walk_data, -healthCode), 
        type = "prob") %>% 
    dplyr::mutate(walking = .pred_parkinsons) %>%
    bind_cols(agg_walk_data) %>%
    dplyr::select(walking, healthCode)

tap_prediction <- predict(tap_model, 
                           select(agg_tap_data, -healthCode), 
                           type = "prob") %>% 
    dplyr::mutate(tapping = .pred_parkinsons) %>%
    bind_cols(agg_tap_data) %>%
    dplyr::select(tapping, healthCode)

conf_scores <- walk_prediction %>% 
    dplyr::full_join(tap_prediction, by = c("healthCode")) %>% 
    tidyr::pivot_longer(!healthCode) %>% 
    dplyr::group_by(healthCode) %>% 
    dplyr::summarise(med_scores = median(value, na.rm = T)) %>%
    dplyr::inner_join(mapping, by = "healthCode") %>%
    dplyr::inner_join(ahpd_scores, by = c("externalId" = "guid")) %>%
    dplyr::group_by(externalId) %>%
    dplyr::summarise(mean_model_score = mean(med_scores, na.rm = T),
                     mean_updrs_tot = mean(UPDRSTOT, na.rm = T),
                     sd_updrs_tot = sd(UPDRSTOT, na.rm = T))

correlation <- cor.test(conf_scores$mean_model_score, conf_scores$mean_updrs_tot)
correlation_string <- glue::glue(
    "corr: {correlation_score}", "\n", "p-value: {p_value}",
    correlation_score = scientific(correlation$estimate[[1]]),
    p_value = scientific(correlation$p.value)
)

conf_scores %>% 
    ggplot(aes(x = mean_model_score, y = mean_updrs_tot)) + 
    geom_smooth(method = lm, 
                color = "skyblue3") + 
    geom_point(alpha = 0.7) + 
    geom_errorbar(aes(ymin = mean_updrs_tot - sd_updrs_tot, 
                      ymax = mean_updrs_tot + sd_updrs_tot),
                  alpha = 0.2) + 
    annotate("text", x=0.25, y=90, label= correlation_string) +
    theme_minimal() +
    labs(title = "Combined Model Correlation Plot",
         subtitle = "Trained on data from mPower V1 test on AHPD",
         y = "Mean UPDRS Scores",
         x = "Mean Model Scores") +
    theme(plot.title = element_text(face = "bold"))
