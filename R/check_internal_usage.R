library(tidyverse)
library(data.table)
library(synapser)
library(ggplot2)
library(ggpubr)
source("R/utils/utils.R")

synapser::synLogin()

scripts_group <- "R|py|m|mlx"
notebooks_group <- "Rmd|ipynb|html"
intermediate_data_group <- "Rdata|hdf|rdata"
tabular_data_group <- "txt|csv|tsv|xlsx"
image_group <- "svg|pdf|png|PNG"

data <- fread("mpower_internal_usage.tsv") %>%
    dplyr::mutate(format = file_ext(name)) %>%
    dplyr::filter(format != 0, format != "") %>%
    dplyr::mutate(Type = case_when(
        stringr::str_detect(format, image_group) ~ "Images (png, svg, jpeg)",
        stringr::str_detect(format, tabular_data_group) ~ "Tabular Data (tsv, csv)",
        stringr::str_detect(format, intermediate_data_group) ~ "Intermediate data (Rdata, Hdf5)",
        stringr::str_detect(format, notebooks_group) ~ "Notebooks (Rmd, Ipynb)",
        stringr::str_detect(format, scripts_group) ~ "Scripts (R, Python, Matlab)",
        TRUE ~ "Others"
    )) %>%
    dplyr::mutate(year = lubridate::year(createdOn))


res <- data %>%
    dplyr::group_by(year, Type) %>%
    dplyr::summarise(n = n())

res %>%
    dplyr::ungroup() %>%
    dplyr::group_by(Type) %>%
    dplyr::mutate(cum_sum = cumsum(n)) %>% 
    dplyr::ungroup() %>%
    ggplot(aes(x = year, y = n, fill = Type)) +
    geom_col() + 
    scale_x_continuous(breaks = unique(data$year)) +
    scale_y_continuous(breaks = c(500, 1000,1500,2000,2500)) +
    geom_hline(yintercept = 2000, linetype = "dashed") +
    scale_fill_brewer(palette = "Set3") +
    labs(
        title = "Number of File Contributions in a Project",
        subtitle = "Queried from mPower Internal Analysis (2016 - Now)",
        y = "# File Contributions") +
    theme_minimal() +
    theme(plot.title = element_text(face = "bold"))
