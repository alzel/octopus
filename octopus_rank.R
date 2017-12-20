library(DBI)
library(zoo)
library(anytime)
library(lubridate)
library(futile.logger)
library(tidyverse)
library(jsonlite)
library(readr)


file_name = "octopus_v2.R"
my_logger = "logger"
flog.appender(appender.file(file = paste(file_name, "log", sep = ".")), name=my_logger)
flog.info("Started running %s", file_name, name = my_logger)

source("config_local.cfg")
con <- try(DBI::dbConnect(RMySQL::MySQL(),
                          user = USER,
                          password = PASSWORD,
                          host = HOST,
                          dbname = DBNAME,
                          port = PORT))

if (class(con) %in% "try-error") {
  err_msg <- geterrmessage()
  flog.error('Can\'t connect to db: %s', gsub("[\r\n]", "", err_msg),name = my_logger)
  stop("Can't proceed without db")
} 

table = "crypto_trends_1d"
trends_db <- tbl(con, table)

trends.data <- trends_db %>% collect()

trends.data1 <- trends.data %>% 
  group_by(coin_id) %>% 
  filter(ymd_hms(date) < ymd_hms(max(date)) - dminutes(17)) #correcting for last dates

trends_data <- trends.data1 %>% 
  select(coin_id, date, hits) %>% 
  dplyr::rename(name = coin_id, value = hits) %>% 
  mutate(date = ymd_hms(date),
         data_type = "trends") 


dataset <- trends_data
non_zeros <- (dataset %>% group_by(name) %>% summarise(zeros = sum(value == 0)) %>% filter(zeros <= 30))$name


threshold = Z_THR # threshold
k1 = N # moving average points (8min * K)

dataset.stats <- dataset %>% filter(name %in% non_zeros) %>% distinct(name, date, value, data_type) %>%
  group_by(name, data_type) %>%
  mutate(avg = rollapply(value, k1, mean, align ="right", na.rm = TRUE, fill = NA),
         std = rollapply(value, k1, sd, align ="right", na.rm = TRUE, fill = NA), 
         z = (value - avg)/std)


toSave <- dataset.stats %>% ungroup() %>% filter(z > threshold) %>% dplyr::select(name, date, z)

file_path = paste(SAVE_DIR, SAFE_FILE, sep = "/")

toSave %>% rename(coin_id = name) %>% ungroup() %>%
  mutate(current_time = ymd_hms(Sys.time()),
         date_diff = as.numeric(current_time - date, units="hours"),
         diff_inv = 1/date_diff,
         scaled_diff_inv = scales::rescale(diff_inv, to = c(1,5)),
         scaled_z = scales::rescale(z, c(1,5)),
         h_mean = 2*scaled_z*scaled_diff_inv/(scaled_z + scaled_diff_inv)) %>%
  toJSON(pretty = T) %>%
  write_lines(file_path)

dbDisconnect(con)

flog.info("Finished running: %s, total peaks %s for %s cryptos", file_name, nrow(toSave), length(unique(toSave$name)), name = my_logger)
q("no")


