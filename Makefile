PARENT_ID_GAIT_WINDOW_5_SECS = syn24991886
PARENT_ID_GAIT_WINDOW_7_SECS = syn24992903
PARENT_ID_GAIT_WINDOW_10_SECS = syn24991912
PARENT_ID_GAIT_WINDOW_12_SECS = syn24991920
PARENT_ID_GAIT_WINDOW_15_SECS = syn24991917

all: extract_gait_v1 extract_gait_v2 extract_gait_passive

#' extract v1 with all window simulation
extract_gait_v1:
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_5_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 500
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_5_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 500
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_5_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 500
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_7_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 750
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_7_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 750
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_7_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 750
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_10_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_10_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_10_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_12_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 1250
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_12_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 1250
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_12_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 1250
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_15_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 1500
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_15_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 1500
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_15_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 1500
	
	
#' extract v1 with all window simulation
extract_gait_v2:
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_5_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -w 500
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_5_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -f "balance_motion.json" -w 500
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_7_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -w 750
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_7_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -f "balance_motion.json" -w 750
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_10_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -w 1000
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_10_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -f "balance_motion.json" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_12_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -w 1250
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_12_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -f "balance_motion.json" -w 1250
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_15_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -w 1500
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_15_secs_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -f "balance_motion.json" -w 1500
	
extract_gait_passive:
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_5_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -w 500
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_7_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -w 750
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_10_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -w 1000
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_12_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -w 1250
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_15_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -w 1500