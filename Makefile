all: extract_walk_test extract_balance_test extract_passive

extract_passive: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn17022539" -o "mpowerV2_passive_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env"
	
extract_walk_test: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_walking_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env"
	
extract_balance_test: 
	Rscript "R/extractPDKitRotationFeatures_V2_Tables.R" -s "syn12514611" -o "mpowerV2_balance_PDKitRotationFeatures.tsv" -p "syn24182621" -e "~/env" -f "balance_motion.json"


