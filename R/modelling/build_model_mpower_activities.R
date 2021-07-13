library(tidyverse)
library(githubr)
library(reticulate)
library(data.table)
library(tidymodels)
library(randomForest)
source("R/utils/utils.R")
set.seed(1000)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### instantiate github #### 
####################################
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "match_healthcodes.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)


####################################
#### instantiate global variables #### 
####################################
OUTPUT_REF <- list(
    walk = list(
        output_file = "age_gender_matched_healthcodes_walk.tsv",
        id = "syn25782772",
        hc = "syn25782946",
        parent_id = "syn25782930"),
    tap = list(
        output_file = "age_gender_matched_healthcodes_tap.tsv",
        id = "syn25782775",
        hc = "syn25782947",
        parent_id = "syn25782930"),
    rest = list(
        output_file ="age_gender_matched_healthcodes_rest.tsv",
        id = "syn25782776",
        hc = "syn25782948",
        parent_id = "syn25782930")
)

run_cv <- function(training_data, 
                   model, 
                   formula,
                   kfold = 10){
    folds <- vfold_cv(training_data, v = kfold)
    rf_wf <- 
        workflow() %>%
        add_model(model) %>%
        add_formula(formula = formula)
    rf_fit_rs <- 
        rf_wf %>% 
        fit_resamples(folds)
    collect_metrics(rf_fit_rs) %>% 
        dplyr::mutate(type = "cv_scores")
}

run_pred_test_set <- function(model, 
                              training_data,
                              testing_data,
                              formula){
    rf_fit <- 
        model %>% 
        fit(formula, data = training_data)
    rf_testing_pred <- 
        predict(rf_fit, testing_data) %>% 
        bind_cols(predict(rf_fit, testing_data, type = "prob")) %>% 
        bind_cols(testing_data %>% select(diagnosis))
    
    dplyr::bind_rows(
        rf_testing_pred %>%                  
            roc_auc(truth = diagnosis, 
                    estimate = .pred_parkinsons),
        rf_testing_pred %>%                 
            accuracy(truth = diagnosis, .pred_class)) %>%
        dplyr::mutate(type = "test_scores")
}

get_features <- function(activity){
    features <- syn$get(OUTPUT_REF[[activity]]$id)$path %>% 
        fread() %>%
        dplyr::select(ends_with("md"), 
                      ends_with("iqr"),
                      healthCode,
                      diagnosis) %>%
        dplyr::filter(diagnosis != "no_answer") %>%
        dplyr::mutate(diagnosis = as.factor(diagnosis)) %>%
        tidyr::drop_na(diagnosis)
    matched_hc <- syn$get(OUTPUT_REF[[activity]]$hc)$path %>% fread()
    features %>% 
        dplyr::filter(healthCode %in% matched_hc$healthCode) %>% 
        dplyr::mutate(diagnosis = relevel(diagnosis, "parkinsons"))
}

run_model_diagnostics <- function(features, model, formula){
    # train test split
    train_test_split <- features %>%
        initial_split(prop = 0.75, strata = diagnosis)
    training <- train_test_split %>% 
        training() 
    testing <- train_test_split %>% 
        testing()
    scores <- list(
        cross_val = 
            run_cv(
                model = model,
                training_data = select(training, -healthCode),
                formula = formula) %>%
            dplyr::mutate(score = mean),
        test_set =
            run_pred_test_set(
                model = model,
                training_data = select(training, -healthCode),
                testing_data = select(testing, -healthCode),
                formula = formula) %>%
            dplyr::mutate(score = .estimate)) %>% 
        dplyr::bind_rows() %>% 
        dplyr::mutate(n_user = length(unique(features$healthCode)), 
                      n_recs = length(unique(features$recordId))) %>%
        dplyr::select(.metric, type, score, n_user, n_recs) %>% 
        dplyr::arrange(.metric)
}

save_model <- function(features, formula, activity, model){
    model <- 
        model %>% 
        fit(formula, data = select(features, -healthCode))
    model_name <- glue::glue("{activity}_model_mpowerV1_freeze.rds")
    output_loc <- file.path("models", model_name)
    saveRDS(model, output_loc)
}


classification_formula <- as.formula("diagnosis ~ .")
get_model_report <- purrr::map(names(OUTPUT_REF), function(activity){
    get_features(activity) %>%
        run_model_diagnostics(model = rand_forest(trees = 1000) %>% 
                                  set_engine("randomForest") %>% 
                                  set_mode("classification"),
                              formula = classification_formula) %>%
        dplyr::mutate(activity = activity)}) %>% 
    dplyr::bind_rows()
save_models <-  purrr::walk(names(OUTPUT_REF), function(activity){
    get_features(activity) %>%
        dplyr::mutate(diagnosis = relevel(diagnosis, "parkinsons")) %>%
        save_model(formula = classification_formula, 
                   model = rand_forest(trees = 1000) %>% 
                       set_engine("randomForest") %>% 
                       set_mode("classification"), 
                   activity = activity)
})


test <- get_features("walk")
features <- test %>% 
    dplyr::select(matches("^x|^AA_speed|^rotation"), 
                  -matches(
                      "^x_speed_of_gait|^y_speed_of_gait|^z_speed_of_gait")) %>% 
    names(.)
test %>% 
    dplyr::select(diagnosis, all_of(features)) %>% 
    tidyr::drop_na() %>% 
    tibble::as_tibble() %>% 
    run_cv(model = model,
           formula = classification_formula)



