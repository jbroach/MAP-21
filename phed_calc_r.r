# Use data.table library
library(data.table)

tb_spd <- fread("C:/Users/saavedrak/metro_work/PHED/Feb2017_test/Feb2017_test.csv")
# Extract hour from datetime timestamp:
tb_spd[, pk_hr := as.integer(strftime(measurement_tstamp, format="%H"))]
tb_peak <- fread("H:/map21/perfMeasures/phed/data/peakingFactors_join_edit.csv")
# Convert text time to standalone integer hour. 
tb_peak[, pk_hour := as.integer(strftime(strptime(startTime, format="%H:%M"), format="%H"))]
tb <- merge(x=tb_spd, y=tb_peak, by.x="pk_hr", by.y="pk_hour")

#tb_meta <- fread("H:/map21/perfMeasures/phed/data/TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv")
#tb_here <- fread("H:/map21/perfMeasures/phed/data/HERE_OR_Static_TriCounty_edit.csv")

#source("C:/Users/saavedrak/metro_work/PHED/phed_calc_r.r")