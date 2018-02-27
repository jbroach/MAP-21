# Use data.table library
library(data.table)

tb <- fread("C:/Users/saavedrak/metro_work/PHED/Feb2017_test/Feb2017_test.csv")
# Filter out weekends:
tb <- tb[!(weekdays(as.Date(measurement_tstamp)) %in% c("Saturday", "Sunday"))]
# Extract hour from datetime timestamp:
tb[, pk_hr := as.integer(strftime(measurement_tstamp, format="%H"))]
tb_peak <- fread("H:/map21/perfMeasures/phed/data/peakingFactors_join_edit.csv")
# Convert text time to standalone integer hour. 
tb_peak[, pk_hour := as.integer(strftime(strptime(startTime, format="%H:%M"), format="%H"))]
tb <- merge(x=tb, y=tb_peak, by.x="pk_hr", by.y="pk_hour", all.x=TRUE)
tb <- tb[pk_hr %in% c(6, 7, 8, 9, 10, 15, 16, 17, 18, 19)]

tb_urban <- fread("H:/map21/perfMeasures/phed/data/urban_tmc.csv")
tb <- merge(x=tb_urban, y=tb, by.x="Tmc", by.y="tmc_code", all.x=TRUE)
tb_meta <- fread("H:/map21/perfMeasures/phed/data/TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv")

tb <- merge(x=tb, y=tb_meta, by.x="tmc_code", by.y="tmc")
#tb_here <- fread("H:/map21/perfMeasures/phed/data/HERE_OR_Static_TriCounty_edit.csv")

#source("C:/Users/saavedrak/metro_work/PHED/phed_calc_r.r")