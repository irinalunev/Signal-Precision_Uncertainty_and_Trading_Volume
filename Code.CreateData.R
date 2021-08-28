library(dplyr)
library(mondate)
library(haven)
library(lubridate)
library(gdata)
library(stargazer)
library(quantreg)
library(lfe)
library(xtable)
library(robustHD)

######STEP 1. Upload data#######
###1.1. I/B/E/S data###
IBES_raw <- read.csv("IBESJune15,2020.csv")
#Create a list of companies with identifications, actuals' announcement dates and times, 
#actual values
data <- unique(IBES_raw[,c("TICKER","CUSIP","OFTIC","CNAME","FPEDATS","ACTUAL","ANNDATS_ACT","ANNTIMS_ACT")])
#Delete companies with missing identifications, with missing or negative actual values
data <- subset(data, OFTIC!="")
data <- subset(data, is.na(ACTUAL)==F)
data <- subset(data, ACTUAL>0)

#Add median forecast variable, forecast standard deviation
data$med_forecast <- NA
data$dispersion <- NA
data$n_analyst <- NA
for (i in 1:nrow(data))
{
  oftic <- data$OFTIC[i]
  fpedats <- data$FPEDATS[i]
  IBES_small <- arrange(subset(IBES_raw,OFTIC==oftic & FPEDATS==fpedats), ANALYS, desc(ANNDATS))
  IBES_small <- IBES_small[!duplicated(IBES_small$ANALYS),]
  data$med_forecast[i] <- median(IBES_small$VALUE)
  data$dispersion[i] <- sd(IBES_small$VALUE)
  data$n_analyst[i] <- nrow(IBES_small)
}


###1.2. CRSP data###
CRSP_daily_raw <- read.csv("CRSPDailyJune15,2020.csv")
#Extract trading days
CRSP_daily_raw$Date <- as.Date(as.character(CRSP_daily_raw$date),"%Y%m%d")
trading_days <- unique(CRSP_daily_raw$Date)

#Add prior fiscal quarter ending date
data$fiscal_period_end <- as.Date(as.character(data$FPEDATS),"%Y%m%d")
data$fiscal_period_end_minus_quarter <- as.Date(mondate(data$fiscal_period_end)-3,"%m/%d/%Y")

#Upload linking table
link_CRSP_IBES <- read.csv("CRSP-IBES.csv")
#Add PERMNO to data table
data_m <- merge(x=data,y=link_CRSP_IBES[,c("TICKER","PERMNO")],by="TICKER",all.x=T)
data_m <- subset(data_m,is.na(PERMNO)==F)
rm(link_CRSP_IBES)

#Add date for price at the prior fiscal quarter ending date 
data_m$date_for_price_prior_quarter[data_m$fiscal_period_end_minus_quarter %in% trading_days] <- as.character(data_m$fiscal_period_end_minus_quarter[data_m$fiscal_period_end_minus_quarter %in% trading_days])
data_m$date_for_price_prior_quarter[is.na(data_m$date_for_price_prior_quarter)==T & (data_m$fiscal_period_end_minus_quarter-1) %in% trading_days] <- as.character(data_m$fiscal_period_end_minus_quarter[is.na(data_m$date_for_price_prior_quarter)==T & (data_m$fiscal_period_end_minus_quarter-1) %in% trading_days]-1)
data_m$date_for_price_prior_quarter[is.na(data_m$date_for_price_prior_quarter)==T & (data_m$fiscal_period_end_minus_quarter-2) %in% trading_days] <- as.character(data_m$fiscal_period_end_minus_quarter[is.na(data_m$date_for_price_prior_quarter)==T & (data_m$fiscal_period_end_minus_quarter-2) %in% trading_days]-2)
data_m$date_for_price_prior_quarter[is.na(data_m$date_for_price_prior_quarter)==T & (data_m$fiscal_period_end_minus_quarter-3) %in% trading_days] <- as.character(data_m$fiscal_period_end_minus_quarter[is.na(data_m$date_for_price_prior_quarter)==T & (data_m$fiscal_period_end_minus_quarter-3) %in% trading_days]-3)
data_m <- subset(data_m,is.na(date_for_price_prior_quarter)==F)

#Add price at the prior fiscal quarter ending date
CRSP_daily_raw <- subset(CRSP_daily_raw, PERMNO %in% data_m$PERMNO)
data_m$date_for_price_prior_quarter <- as.Date(data_m$date_for_price_prior_quarter)
CRSP_daily_raw <- rename(CRSP_daily_raw, "date_for_price_prior_quarter"=Date)
data_n <- merge(x=data_m,y=CRSP_daily_raw, by=c("PERMNO","date_for_price_prior_quarter"),all.x=T)
rm(data_m)
data_n <- data_n[,c("PERMNO","TICKER.x","CUSIP.x","OFTIC","CNAME","FPEDATS","ACTUAL",
                    "ANNDATS_ACT","ANNTIMS_ACT","med_forecast","dispersion","n_analyst",
                    "fiscal_period_end","PRC")] %>% subset(is.na(PRC)==F) %>% rename("PRC_prior_quarter"=PRC)
data_w <- data_n[!duplicated(data_n[colnames(data_n)[1:14]]),]
rm(data_n)
#Keep only firms with price at the end of prior fiscal quarter greater than $5
data_w <- subset(data_w,PRC_prior_quarter>=5)

#Add VOL_by_shrout - trading volume on the announcement date divided by shares outstanding
CRSP_daily_raw <- subset(CRSP_daily_raw,is.na(VOL)==F)
CRSP_daily_raw$VOL_by_shrout <- CRSP_daily_raw$VOL/CRSP_daily_raw$SHROUT
#Remove dates when VOL_by_shrout is zero
CRSP_daily_raw <- subset(CRSP_daily_raw,VOL_by_shrout>0)

#Add date for VOL_by_shrout: announcement date
data_w <- subset(data_w,is.na(ANNDATS_ACT)==F)
data_w$annday_real <- NA
data_w$announcement_actual_date <- as.Date(as.character(data_w$ANNDATS_ACT),"%Y%m%d")
#Announcement hours
data_w$annhour <- as.numeric(sub("\\:.*", "", data_w$ANNTIMS_ACT))+1
data_w$announcement_actual_date_hour_corrected <- NA
data_w$announcement_actual_date_hour_corrected[data_w$annhour<=16] <- as.character(data_w$announcement_actual_date[data_w$annhour<=16])
data_w$announcement_actual_date_hour_corrected[data_w$annhour>16] <- as.character(data_w$announcement_actual_date[data_w$annhour>16]+1)

data_w$annday_real[data_w$announcement_actual_date_hour_corrected %in% as.character(trading_days)] <- as.character(data_w$announcement_actual_date_hour_corrected[data_w$announcement_actual_date_hour_corrected %in% as.character(trading_days)])
data_w <- subset(data_w,is.na(annday_real)==F)

#Add VOL_by_shrout_0
data_w$annday_real <- as.Date(data_w$annday_real)
CRSP_daily_raw <- rename(CRSP_daily_raw, "annday_real"=date_for_price_prior_quarter)
data_f <- merge(x=data_w,y=CRSP_daily_raw[,c("PERMNO","annday_real","VOL_by_shrout")],by=c("PERMNO","annday_real"),all.x=T)
rm(data_w)

#Add VOL_by_shrout_1
data_f$annday_real_plus1 <- NA
data_f$annday_real_plus1[(data_f$annday_real+1) %in% trading_days] <- as.character(data_f$annday_real[(data_f$annday_real+1) %in% trading_days]+1)
data_f$annday_real_plus1[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+2) %in% trading_days] <- as.character(data_f$annday_real[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+2) %in% trading_days]+2)
data_f$annday_real_plus1[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+3) %in% trading_days] <- as.character(data_f$annday_real[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+3) %in% trading_days]+3)
data_f$annday_real_plus1[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+4) %in% trading_days] <- as.character(data_f$annday_real[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+4) %in% trading_days]+4)
data_f$annday_real_plus1[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+5) %in% trading_days] <- as.character(data_f$annday_real[is.na(data_f$annday_real_plus1)==T & (data_f$annday_real+5) %in% trading_days]+5)
data_f <- subset(data_f,is.na(annday_real_plus1)==F)
data_f <- rename(data_f,"VOL_by_shrout_0"=VOL_by_shrout)
CRSP_daily_raw <- rename(CRSP_daily_raw, "annday_real_plus1"=annday_real)
data_f$annday_real_plus1 <- as.Date(data_f$annday_real_plus1)
data_t <- merge(x=data_f,y=CRSP_daily_raw[,c("PERMNO","annday_real_plus1","VOL_by_shrout")],by=c("PERMNO","annday_real_plus1"),all.x=T)
rm(data_f)
data_t <- rename(data_t, "VOL_by_shrout_plus1"="VOL_by_shrout")
data_t <- subset(data_t,is.na(VOL_by_shrout_0)==F) %>% subset(is.na(VOL_by_shrout_plus1)==F)
data_t <- data_t[,c("PERMNO","annday_real","TICKER.x","CUSIP.x","OFTIC","CNAME","FPEDATS",
                    "ACTUAL","ANNDATS_ACT","ANNTIMS_ACT","med_forecast","dispersion",
                    "n_analyst","fiscal_period_end","PRC_prior_quarter","annhour",
                    "announcement_actual_date","VOL_by_shrout_0","VOL_by_shrout_plus1")]

#Add mVOL and dVOL - mean and sd of TV during [-240,-5] period
data_t$mVOL <- NA
data_t$dVOL <- NA
CRSP_daily_raw <- rename(CRSP_daily_raw, "Date"=annday_real_plus1)
for (i in 1:nrow(data_t))
{
  permnoi <- data_t$PERMNO[i]
  datei <- data_t$annday_real[i]
  CRSP_small <- CRSP_daily_raw %>% subset(PERMNO==permnoi) %>% arrange(desc(Date))
  CRSP_small <- CRSP_small[(which(grepl(datei, CRSP_small$Date))+5):(which(grepl(datei, CRSP_small$Date))+240),]
  data_t$mVOL[i] <- mean(CRSP_small$VOL_by_shrout)
  data_t$dVOL[i] <- sd(CRSP_small$VOL_by_shrout)
}

#Remove observations with missing mVOL and dVOL
data_t <- subset(data_t, is.na(mVOL)==F)

###1.3 Compustat data###
Compustat_raw <- read.csv("CompustatQuarterlyJune24,2020.csv")
link_CRSP_Comp <- read.csv("CRSP_Compustat_linkJune24,2020.csv")
link_CRSP_Comp <- rename(link_CRSP_Comp,"gvkey"=GVKEY)
Compustat <- merge(x=Compustat_raw,y=link_CRSP_Comp[,c("gvkey","PERMCO")],by="gvkey", all.x=T)

#Add PERMCO to data_t
CRSP_daily_raw <- rename(CRSP_daily_raw, "ANNDATS_ACT"=date)
data_x <- merge(x=data_t, y=CRSP_daily_raw[,c("PERMNO","PERMCO","ANNDATS_ACT")], by=c("PERMNO","ANNDATS_ACT"),all.x=T)
CRSP_daily_raw <- rename(CRSP_daily_raw, "date"=ANNDATS_ACT)
data_x <- subset(data_x, is.na(PERMCO)==F)

#Merge data_x with Compustat
data_x <- rename(data_x, "ibtic"=TICKER.x)
data_x$fiscal_period_end_minus_quarter <- as.Date(mondate(data_x$fiscal_period_end)-3,"%m/%d/%Y")
data_x$fiscal_period_end_minus_quarter <- ceiling_date(ymd(data_x$fiscal_period_end_minus_quarter), "month")-1
Compustat$fiscal_period_end_minus_quarter <- as.Date(as.character(Compustat$datadate), "%Y%m%d")
Compustat$fiscal_period_end_minus_quarter <- ceiling_date(ymd(Compustat$fiscal_period_end_minus_quarter), "month")-1
data_h <- merge(x=data_x, y=Compustat[,c("conm","PERMCO","fiscal_period_end_minus_quarter","atq","prccq","cshoq","ceqq","mkvaltq")], by=c("PERMCO","fiscal_period_end_minus_quarter"), all.x=T)
data_h <- data_h[!duplicated(data_h[colnames(data_h)[1:22]]),]
rm(data_t)
#Leave only firms that we have in Compustat
data_l <- subset(data_h, is.na(cshoq)==F)

###1.4 Pastor-Strambaugh liquidity data###
ps_liq_raw <- read_sas("liq_ps.sas7bdat")
ps_level_monthly <- ts(ps_liq_raw$PS_LEVEL[3:nrow(ps_liq_raw)], start = c(1962, 4), frequency = 12)
ps_level_quarterly <- aggregate(ps_level_monthly, nfrequency = 4)
nth_element <- function(vector, starting_position, n) { 
  vector[seq(starting_position, length(vector), n)] 
}
quarters <- nth_element(ps_liq_raw$DATE, 5, 3)
quarters[quarters=="1994-12-30"] <- "1994-12-31"
ps_level_quarterly <- data.frame(quarters,ps_level_quarterly)
ps_level_quarterly$quarters <- ceiling_date(ymd(ps_level_quarterly$quarters), "month")-1

data_l <- merge(x=data_l, y=ps_level_quarterly, by.x="fiscal_period_end_minus_quarter",by.y="quarters",all.x=T)
data_l <- subset(data_l,is.na(ps_level_quarterly)==F)

###1.5 Final dataset###
final <- data_l
write.csv(final,file="finalJune24,2020.csv")




