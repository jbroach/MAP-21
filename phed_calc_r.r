# Use data.table library
library(data.table)
library(lubridate) # Speed up slow time parsing behavior.

per.capita.TED <- function(sum.11mo) {
  year.adjusted.TED <- (sum.11mo / 11) + sum.11mo
  pop.PDX <- 1577456
  return(year.adjusted.TED / pop.PDX)
}

TED.summation <- function(tb.teds) {
  VOCa <- 1.4
  VOCb <- 10
  VOCt <- 1
  tb.teds[, AVOc := pct.auto * VOCa]
  tb.teds[, AVOb := pct.bus * VOCb]
  tb.teds[, AVOt := pct.truck * VOCt]
  tb.teds[, TED  := gp.TED.seg * (AVOc + AVOb + AVOt)]
  return(tb.teds)
}

total.excessive.delay <- function(tb.ted) {
  tb.ted[, TED.seg := ED * PK.HR]
  tb.ted.a <- tb.ted[, j = list(gp.TED.seg =  sum(TED.seg, na.rm = TRUE),
                                pct.auto   =  max(pct.auto),
                                pct.bus    =  max(pct.bus),
                                pct.truck  =  max(pct.truck)
                              ), by = Tmc]
  return(tb.ted.a)
}

peak.hr <- function(tb.pk) {
  tb.pk[, PK.HR := dir.aadt * `2015_15-min_Combined`]
  return(tb.pk)
}

excessive.delay <- function(tb.ed) {
  tb.ed[, ED := RSD / 3600]
  tb.ed[ED < 0, ED := 0]
  return(tb.ed)
}

RSD <- function(tb.rsd) {
  tb.rsd[, RSD := travel_time_seconds - SD]
  tb.rsd[RSD < 0, RSD := 0]
  return(tb.rsd)
}

segment.delay <- function(tb.sd) {
  tb.sd[, SD := (miles / TS) * 3600]
  return(tb.sd)
}

AADT.splits <- function(tb.spl) {
  tb.spl[, dir.aadt := aadt / faciltype]
  tb.spl[, aadt.auto := dir.aadt - (aadt_singl + aadt_combi)]
  tb.spl[, pct.auto := aadt.auto / dir.aadt]
  tb.spl[, pct.bus := aadt_singl / dir.aadt]
  tb.spl[, pct.truck := aadt_combi / dir.aadt]
  return(tb.spl)
}

threshold.speed <- function(tb.ts) {
  tb.ts[, TS := SPEED_LIMIT * .6]
  tb.ts[TS < 20 | is.na(TS), TS := 20]
  return(tb.ts)
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

drive.path <- "original_data/"
quarters <- c("2017Q1", "2017Q2", "2017Q3", "2017Q4")
folder.end <- "_TriCounty_Metro_15-min"
file.end <- "_NPMRDS (Trucks and passenger vehicles).csv"

for (q in quarters) {
  filename <- paste(q, folder.end, file.end, sep = "")
  path <- paste(q, folder.end, sep = "")
  full.path <- paste(path, filename, sep = "/")
  tb.temp <- fread(paste(drive.path, full.path, sep = ""))
  tb <- rbind(tb, tb.temp)
}

#tb <- fread("C:/Users/saavedrak/metro_work/PHED/Feb2017_test/Feb2017_test.csv")
# Filter out weekends:
tb <- tb[!(weekdays(ymd_hms(measurement_tstamp)) %in% c("Saturday", "Sunday"))]
# Extract hour from datetime timestamp:
tb[, pk.hr := hour(measurement_tstamp)]
tb.peak <- fread("peakingFactors_join_edit.csv")
# Convert text time to standalone integer hour. 
tb.peak[, pk.hour := as.integer(hour(parse_date_time(startTime, orders="HM")))]
# Left join to main data file.
tb <- merge(x = tb, y = tb.peak, by.x = "pk.hr", by.y = "pk.hour",
            all.x = TRUE)
tb <- tb[pk.hr %in% c(6, 7, 8, 9, 10, 15, 16, 17, 18, 19)]
# Inner join with urban areas only
tb.urban <- fread("urban_tmc.csv")
tb <- merge(x = tb.urban, y = tb, by.x = "Tmc", by.y = "tmc_code")
# Merge metadata
tb.meta <- fread(
  "TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv")
tb <- merge(x = tb, y = tb.meta, by.x = "Tmc", by.y = "tmc")
# Join HERE data
tb.here <- fread("HERE_OR_Static_TriCounty_edit.csv")
tb <- merge(x = tb, y = tb.here, by.x = "Tmc", by.y = "TMC_HERE", all.x = TRUE)

tb <- threshold.speed(tb)
tb <- AADT.splits(tb)
tb <- segment.delay(tb)
tb <- RSD(tb)
tb <- excessive.delay(tb)
tb <- peak.hr(tb)
tb <- total.excessive.delay(tb)
tb <- TED.summation(tb)

result <- per.capita.TED(sum(tb[, TED]))
print(result)

end.time <- Sys.time()
total.time <- end.time - start.time
print(total.time)
