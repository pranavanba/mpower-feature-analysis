all: extract_walk_test extract_balance_test extract_passive filter aggregate

extract_passive: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env"
	
extract_walk_test: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env"
	
extract_balance_test: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env" -f "balance_motion.json"

# used if data is available (QnD)	
filter:
	Rscript "R/aggregate_features.R" -f "syn24182637" -o "mpowerV2_walking_PDKitRotationFeatures_filtered.tsv" -p "syn24182621"
	Rscript "R/aggregate_features.R" -f "syn24182646" -o "mpowerV2_balance_PDKitRotationFeatures_filtered.tsv" -p "syn24182621"
	Rscript "R/aggregate_features.R" -f "syn24182636" -o "mpowerV2_passive_PDKitRotationFeatures_filtered.tsv" -p "syn24182621"
	
# used if data is available (QnD)	
aggregate:
	Rscript "R/aggregate_features.R" -f "syn24182637" -o "mpowerV2_walking_PDKitRotationFeatures_userAgg.tsv" -p "syn24184522" -agg
	Rscript "R/aggregate_features.R" -f "syn24182646" -o "mpowerV2_balance_PDKitRotationFeatures_userAgg.tsv" -p "syn24184522" -agg
	Rscript "R/aggregate_features.R" -f "syn24182636" -o "mpowerV2_passive_PDKitRotationFeatures_userAgg.tsv" -p "syn24184522" -agg
	
	


