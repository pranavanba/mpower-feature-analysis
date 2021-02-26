PARENT_ID_GAIT_WINDOW_5_SECS = syn24991886
PARENT_ID_GAIT_WINDOW_7_SECS = syn24875731
PARENT_ID_GAIT_WINDOW_10_SECS = syn24991912
PARENT_ID_GAIT_WINDOW_12_SECS = syn24991920
PARENT_ID_GAIT_WINDOW_15_SECS = syn24991917



all: extract_walk_test extract_balance_test extract_passive filter aggregate

#' extract v1 with all window simulation
extract_gait_v1:
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_5_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items"
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_5_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items"
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_5_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_5_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items"
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_7_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 700
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_7_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 700
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_7_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_7_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 700
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_10_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_10_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_10_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_10_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_12_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 1250
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_12_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 1250
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_12_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_12_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 1250
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_15_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 1500
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_15_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -e "~/env" -f "deviceMotion_walking_return.json.items" -w 1500
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_15_secs_window_PDKitRotationFeatures.tsv" -p $(PARENT_ID_GAIT_WINDOW_15_SECS) -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 1500



extract_v2_5_secs:
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env"
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env" -f "balance_motion.json"

extract_passive_5_secs: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env"
	
	
# recipe for extraction based on 7 seconds window
extract_v1_7_secs:
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 700
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "deviceMotion_walking_return.json.items" -w 700
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 700
extract_v2_7_secs:
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -w 700
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "balance_motion.json" -w 700
extract_passive_7_secs: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -w 70
	
	
# recipe for extraction based on 7 seconds window
extract_v1_10_secs:
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_outbound_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "deviceMotion_walking_outbound.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_return_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "deviceMotion_walking_return.json.items" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V1_Tables.R" -s "syn10308918" -o "mpowerV1_walking_rest_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "deviceMotion_walking_rest.json.items" -w 1000
extract_v2_10_secs:
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -w 1000
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -f "balance_motion.json" -w 1000
extract_passive_10_secs: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_7secs_window_PDKitRotationFeatures.tsv" -p "syn24875731" -e "~/env" -w 1000