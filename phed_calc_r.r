# Use data.table library
library(data.table)
library(lubridate) # Speed up slow time parsing behavior.

per_capita_TED <- function(sum_11_mo) {
  year_adjusted_TED <- (sum_11_mo / 11) + sum_11_mo
  pop_PDX <- 1577456
  return(year_adjusted_TED / pop_PDX)
}

TED_summation <- function(tb_teds) {
  VOCa <- 1.4
  VOCb <- 10
  VOCt <- 1
  tb_teds[, AVOc := pct_auto * VOCa]
  tb_teds[, AVOb := pct_bus * VOCb]
  tb_teds[, AVOt := pct_truck * VOCt]
  tb_teds[, TED  := gp_TED_seg * (AVOc + AVOb + AVOt)]
  return(tb_teds)
}

total_excessive_delay <- function(tb_ted) {
  tb_ted[, TED_seg := ED * PK_HR]
  tb_ted.a <- tb_ted[, j = list(gp_TED_seg =  sum(TED_seg, na.rm = TRUE),
                                pct_auto   =  max(pct_auto),
                                pct_bus    =  max(pct_bus),
                                pct_truck  =  max(pct_truck)
                              ), by = Tmc]
  return(tb_ted.a)
}

peak_hr <- function(tb_pk) {
  tb_pk[, PK_HR := dir_aadt * `2015_15-min_Combined`]
  return(tb_pk)
}

excessive_delay <- function(tb_ed) {
  tb_ed[, ED := RSD / 3600]
  tb_ed[ED < 0, ED := 0]
  return(tb_ed)
}

RSD <- function(tb_rsd) {
  tb_rsd[, RSD := travel_time_seconds - SD]
  tb_rsd[RSD < 0, RSD := 0]
  return(tb_rsd)
}

segment_delay <- function(tb_sd) {
  tb_sd[, SD := (miles / TS) * 3600]
  return(tb_sd)
}

AADT_splits <- function(tb_spl) {
  tb_spl[, dir_aadt := aadt / faciltype]
  tb_spl[, aadt_auto := dir_aadt - (aadt_singl + aadt_combi)]
  tb_spl[, pct_auto := aadt_auto / dir_aadt]
  tb_spl[, pct_bus := aadt_singl / dir_aadt]
  tb_spl[, pct_truck := aadt_combi / dir_aadt]
  return(tb_spl)
}

threshold_speed <- function(tb_ts) {
  tb_ts[, TS := SPEED_LIMIT * .6]
  tb_ts[TS < 20 | is.na(TS), TS := 20]
  return(tb_ts)
}

###############################################################################
# Begin Main Script
###############################################################################

start.time <- Sys.time()

tb <- data.table(
  tmc_code = character(),
  measurement_tstamp = character(),
  speed = double(),
  average_speed = double(),
  reference_speed = double(),
  travel_time_seconds = double()
)

setwd("H:/map21/perfMeasures/phed/data")

drive_path <- "original_data/"
quarters <- c("2017Q1", "2017Q2", "2017Q3", "2017Q4")
folder_end <- "_TriCounty_Metro_15-min"
file_end <- "_NPMRDS (Trucks and passenger vehicles).csv"

for (q in quarters) {
  filename <- paste(q, folder_end, file_end, sep = "")
  path <- paste(q, folder_end, sep = "")
  full_path <- paste(path, filename, sep = "/")
  tb_temp <- fread(paste(drive_path, full_path, sep = ""))
  tb <- rbind(tb, tb_temp)
}

#tb <- fread("C:/Users/saavedrak/metro_work/PHED/Feb2017_test/Feb2017_test.csv")
# Filter out weekends:
tb <- tb[!(weekdays(ymd_hms(measurement_tstamp)) %in% c("Saturday", "Sunday"))]
# Extract hour from datetime timestamp:
tb[, pk_hr := hour(measurement_tstamp)]
tb_peak <- fread("peakingFactors_join_edit.csv")
# Convert text time to standalone integer hour. 
tb_peak[, pk_hour := as.integer(hour(parse_date_time(startTime, orders="HM")))]
# Left join to main data file.
tb <- merge(x = tb, y = tb_peak, by.x = "pk_hr", by.y = "pk_hour",
            all.x = TRUE)
tb <- tb[pk_hr %in% c(6, 7, 8, 9, 10, 15, 16, 17, 18, 19)]
# Inner join with urban areas only
tb_urban <- fread("urban_tmc.csv")
tb <- merge(x = tb_urban, y = tb, by.x = "Tmc", by.y = "tmc_code")
# Merge metadata
tb_meta <- fread(
  "TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv")
tb <- merge(x = tb, y = tb_meta, by.x = "Tmc", by.y = "tmc")
# Join HERE data
tb_here <- fread("HERE_OR_Static_TriCounty_edit.csv")
tb <- merge(x = tb, y = tb_here, by.x = "Tmc", by.y = "TMC_HERE", all.x = TRUE)

tb <- threshold_speed(tb)
tb <- AADT_splits(tb)
tb <- segment_delay(tb)
tb <- RSD(tb)
tb <- excessive_delay(tb)
tb <- peak_hr(tb)
tb <- total_excessive_delay(tb)
tb <- TED_summation(tb)

result <- per_capita_TED(sum(tb[, TED]))
print(result)

end.time <- Sys.time()
total.time <- end.time - start.time
print(total.time)
