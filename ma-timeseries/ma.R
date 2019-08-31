library(data.table)
library(lubridate)

setwd("~/mds/git-repos/python-snippets/ma-timeseries/")

compute_ma <- function(dt, window) {
  dt <- dt[, paste0("ma", window) := lapply(.SD, frollmean, n=window, fill=NA), 
           by=c("Symbol"), .SDcols=c("Adj Close")]
}

dt <- fread("./data/stocks.csv") 
dt$Date <- ymd(dt$Date)
dt <- compute_ma(dt, 20)
dt <- compute_ma(dt, 50)

dt[Symbol=="GOOG" & year(Date) == 2018][1:30]
