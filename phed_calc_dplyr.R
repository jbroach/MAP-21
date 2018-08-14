library(tidyverse)
library(magrittr)
library(lubridate)


###############################################################################
# Begin Main Script
###############################################################################

start.time <- Sys.time()

setwd("H:/map21/perfMeasures/phed/data")

drive_path <- "original_data/"
quarters <- c("2017Q1", "2017Q2", "2017Q3", "2017Q4")
folder_end <- "_TriCounty_Metro_15-min"
file_end <- "_NPMRDS (Trucks and passenger vehicles).csv"

# Initialize empty vector
tb <- vector(mode="numeric", length=0)

for (q in quarters) {
  filename <- paste(q, folder_end, file_end, sep = "")
  path <- paste(q, folder_end, sep = "")
  full_path <- paste(path, filename, sep = "/")
  tb_temp <- read_csv(paste(drive_path, full_path, sep = ""))
  tb <- bind_rows(tb, tb_temp)
  rm(tb_temp)
}

tb %<>% 
  # Create day field & filter out weekends.
  mutate(day = wday(ymd_hms(measurement_tstamp), label=TRUE)) %>% 
  filter(!day %in% c("Sat", "Sun")) %>% 
  # Create integer hour field
  mutate(pk_hr = as.integer(hour(measurement_tstamp)))

  
# Join CSV and filter 
tb_peak <- read_csv("peakingFactors_join_edit.csv") %>% 
  mutate(pk_hour = hour(startTime))

tb %<>%  
  left_join(tb_peak, by=c("pk_hr" = "pk_hour")) %>% 
  filter(pk_hr %in% c(6, 7, 8, 9, 10, 15, 16, 17, 18, 19)) 

tb_urban <- read_csv("urban_tmc.csv") %>% 
  mutate(tb_tmc = Tmc)
tb %<>% 
  inner_join(tb_urban, by=c("tmc_code" = "Tmc"))
  
tb_meta <-  read_csv(
  "TMC_Identification_NPMRDS (Trucks and passenger vehicles).csv")
tb %<>%
  left_join(tb_meta, by=c("tb_tmc" = "tmc"))

tb_here <- read_csv("HERE_OR_Static_TriCounty_edit.csv")
tb %<>%
  left_join(tb_here, by=c("tb_tmc" = "TMC_HERE"))

VOCa <- 1.4
VOCb <- 10
VOCt <- 1

tb %<>%
  
  # Threshold speed calc
  mutate(TS = SPEED_LIMIT * .6) %>%
  mutate(TS = ifelse(TS < 20 | is.na(TS), 20, TS)) %>% 
  
  # AADT splits
  mutate(dir_aadt = aadt / faciltype) %>% 
  mutate(aadt_auto = dir_aadt - (aadt_singl + aadt_combi)) %>% 
  mutate(pct_auto = aadt_auto / dir_aadt) %>% 
  mutate(pct_bus = aadt_singl / dir_aadt) %>% 
  mutate(pct_truck = aadt_combi / dir_aadt) %>% 

  # Segment delay    
  mutate(SD = (miles / TS) * 3600) %>% 
  
  # RSD
  mutate(RSD = travel_time_seconds - SD) %>% 
  mutate(RSD = ifelse(RSD < 0, 0, RSD)) %>% 
  
  # Excessive Delay
  mutate(ED = RSD / 3600) %>% 
  mutate(ED = ifelse(ED < 0, 0, ED)) %>% 

  # Peak Hour
  mutate(PK_HR = dir_aadt * `2015_15-min_Combined`) %>% 
  
  # Total Excessive Delay 
  mutate(TED_seg = ED * PK_HR) %>% 
  group_by(tmc_code) %>% 
  summarize(gp_TED_seg = sum(TED_seg, na.rm=TRUE), 
            pct_auto = max(pct_auto),
            pct_bus = max(pct_bus),
            pct_truck = max(pct_truck)) %>% 
    
  # TED Summation
  mutate(AVOc = pct_auto * VOCa) %>% 
  mutate(AVOb = pct_bus * VOCb) %>% 
  mutate(AVOt = pct_truck * VOCt) %>% 
  mutate(TED  = gp_TED_seg * (AVOc + AVOb + AVOt))

  
sum_11_mo <- sum(tb$TED)
year_adjusted_TED <- (sum_11_mo / 11) + sum_11_mo
pop_PDX <- 1577456
print(year_adjusted_TED / pop_PDX)

end.time <- Sys.time()
total.time <- end.time - start.time
print(total.time)