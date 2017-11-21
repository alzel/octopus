rm(list = ls())

library(jsonlite)
library(gtrendsR)
library(DBI)
library(tidyverse)
library(dbplyr)
library(futile.logger)

# Set proxy globally (case sensitive!)
Sys.setenv(http_proxy = "socks5://localhost:9050")
Sys.setenv(HTTPS_PROXY = "socks5://localhost:9050")

file_name = "insert_db.R"
my_logger = "logger"
flog.appender(appender.file("insert_db.log"), name=my_logger)
flog.info("Started running %s", file_name, name = my_logger)

minDate = paste(as.character(as.numeric(as.POSIXct("2010-01-01 00:00:00 EST"))),"000", sep = "")
maxDate = paste(as.character(round(as.numeric(Sys.time()))), "000", sep = "")

my_write = function(con, data_trends, pool_id, sys_time, suffix = "_test" ) {
  #writes data_trends to database
  for (i in seq_along(data_trends) ) {
    tbl_name = paste(names(data_trends)[i], suffix, sep = "")
    if (!is.null(data_trends[[i]])) {
      tbl_df = data_trends[[i]]
      tbl_df = add_column(tbl_df, sys_time, .before = 1)
      tbl_df = add_column(tbl_df, pool_id, .before = 1)
      status = try(DBI::dbWriteTable(conn = con, name = tbl_name, value = tbl_df, row.names = FALSE, append=T))
      
      if (class(status) %in% "try-error") {
        err_msg <- geterrmessage()
        flog.error('Error obtaining initial trends (relative): %s', gsub("[\r\n]", "", err_msg),name = my_logger)
      } else if (status == TRUE) {
        flog.info('Succesfully inserted in %s total %s records', tbl_name, nrow(tbl_df),name = my_logger)
      } 
    }
  }
}

my_write2 = function(con, data_trends, pool_id, sys_time, table = "interest_over_time", suffix = "_test" ) {
  
  #writes data_trends to database
  if(!is.null(data_trends[[table]])) {
    tbl_name = paste(table, suffix, sep = "")
    
    tbl_df = data_trends[[table]]
    tbl_df = add_column(tbl_df, sys_time, .before = 1)
    tbl_df = add_column(tbl_df, pool_id, .before = 1)
    status = try(DBI::dbWriteTable(conn = con, name = tbl_name, value = tbl_df, row.names = FALSE, append=T))
    
    print (status)
    if (class(status) %in% "try-error") {
      err_msg <- geterrmessage()
      flog.error('Error obtaining initial trends (relative): %s', gsub("[\r\n]", "", err_msg),name = my_logger)
    } else if (status == TRUE) {
      flog.info('Succesfully inserted in %s total %s records', tbl_name, nrow(tbl_df),name = my_logger)
    } 
  }
}

source("config_local.cfg")

con <- try(DBI::dbConnect(RMySQL::MySQL(),
                          user = USER,
                          password = PASSWORD,
                          host = HOST,
                          dbname = DBNAME,
                          port = PORT))

if(class(con) %in% "try-error") {
  err_msg <- geterrmessage()
  flog.error('Failing to connect to database: %s', gsub("[\r\n]", "", err_msg),name = my_logger)
  stop(err_msg)
}

pool_id = NULL

if ( any(!c("interest_over_time_rel", "interest_by_region_abs") %in% DBI::dbListTables(con))) {
  
  pool_id <- 0  
  sys_time <- Sys.time()
  
  relative <- try(gtrends(c("bitcoin", "ethereum"), time = "all"))
  absolute <- try(gtrends(c("bitcoin"), time = "all"))
  
  if (class(relative) %in% "try-error") {
    err_msg <- geterrmessage()
    flog.error('Error obtaining initial trends (relative): %s', gsub("[\r\n]", "", err_msg),name = my_logger)
    stop(err_msg)
  }
   
  my_write(con = con, data_trends = relative, pool_id = 0, sys_time = Sys.time(), suffix = "_rel" )
  my_write(con = con, data_trends = absolute, pool_id = 0, sys_time = Sys.time(), suffix = "_abs" )
  
  flog.info("Writing first database entry", name = my_logger)
  
} else if (("interest_over_time_abs" %in% DBI::dbListTables(con)) && ("interest_over_time_abs" %in% DBI::dbListTables(con))) {
  
  tmp = dbGetQuery(conn = con, statement = "SELECT MAX(pool_id) FROM interest_over_time_abs;")
  pool_id <- tmp$`MAX(pool_id)` + 1 
} else {
  stop("SOmething wrong")
}


current <- try(fromJSON("https://api.coinmarketcap.com/v1/ticker/?&limit=1000"))
if (class(current) %in% "try-error") {
  flog.error("Can't access coinmarketcap.com, getting", name = my_logger)
  tmp = dbGetQuery(conn = con, statement = "SELECT DISTINCT(keyword) FROM interest_over_time_abs;")
  current = data.frame(name = tmp$keyword)
}


lapply(current$name, FUN = function(x) {
  
  relative4h <- try(gtrends(c("ethereum", x), time = "now 4-H"))
  if ((class(relative4h) %in% "try-error")){
    err_msg <- geterrmessage()
    flog.error('Error obtaining trends for: %s %s', x, gsub("[\r\n]", "", err_msg),name = my_logger)
  } else {
    flog.info("Inserting trends for %s for pool_id %s", x, pool_id, name = my_logger)
    my_write2(con = con, data_trends = relative4h, pool_id = pool_id, 
              table = "interest_over_time",
              sys_time = Sys.time(), suffix = "_rel")
  }
  
  actual4h <- try(gtrends(x, time = "now 4-H"))
  if ((class(actual4h) %in% "try-error")){
    err_msg <- geterrmessage()
    flog.error('Error obtaining trends for: %s %s', x, gsub("[\r\n]", "", err_msg),name = my_logger)
  } else {
    flog.info("Inserting trends for %s for pool_id %s", x, pool_id, name = my_logger)
    my_write2(con = con, data_trends = actual4h, pool_id = pool_id, 
              table = "interest_over_time",
              sys_time = Sys.time(), suffix = "_abs")
  }
   
  actual1d <- try(gtrends(x, time = "now 1-d"))
  
  if (class(actual1d) %in% "try-error" ){
    err_msg <- geterrmessage()
    flog.error('Error obtaining trends for: %s %s', x, gsub("[\r\n]", "", err_msg),name = my_logger)
  } else {
    flog.info("Inserting single trends for %s for pool_id %s", x, pool_id, name = my_logger)
    
    query <- paste("DELETE FROM interest_over_time_abs_one WHERE keyword = '", x, "';", sep = "")
    status = try(dbGetQuery(conn = con, statement = query))
    
    if( !(class(status) %in% "try-error")) {
      my_write2(con = con, data_trends = actual1d, 
                pool_id = pool_id, sys_time = Sys.time(), 
                table = "interest_over_time",
                suffix = "_abs_one" )   
      
    }
  }
  #Sys.sleep(round(runif(1, 1, 2)))
}) 
dbDisconnect(con)
flog.info("Finished running %s with pool_id %s", file_name, pool_id, name = my_logger)
file.remove(".RData") 
q("n")
