# Use data.table library
# library(data.table)

tb_speed <- read.csv("C:/Users/saavedrak/metro_work/PHED/Feb2017_test/Feb2017_test.csv")
# Format as DateTime equivalent:
tb_speed$pk_hr <- as.integer(strftime(tb_speed$measurement_tstamp, format="%H"))

#tb_peak <- fread("H:/map21/perfMeasures/phed/data/urban_tmc.csv")
#tb_meta <- fread("H:/map21/perfMeasures/phed/data/TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv")
#tb_here <- fread("H:/map21/perfMeasures/phed/data/HERE_OR_Static_TriCounty_edit.csv")
#merge(x=tb_speed, y=tb_peak, by.x="tmc_code", by.y=

#source("C:/Users/saavedrak/metro_work/PHED/phed_calc_r.r")