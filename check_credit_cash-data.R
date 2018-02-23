library(RMySQL)
library(plyr)
library(openxlsx)
library(data.table)
source("feature.R",encoding = 'UTF-8')
source("con_mysql.R",encoding = 'UTF-8')
source("utf82cp936.R",encoding = 'UTF-8')
ptm <- proc.time()
# 取出所有credit数据和cash数据
con <- dbConnect(MySQL(),host="192.168.0.20",dbname="db_traffic",user="wangxx",password="Wangxx@2017")
res <- dbSendQuery(con, "select * from send_down_credit_date;")
data.credit <- dbFetch(res, n=-1)
# res <- dbSendQuery(con, "select * from send_down_cash_date;")
# data.cash <- dbFetch(res, n=-1)
dbDisconnect(con)
data.credit <- utf82cp936(data.credit)
data.credit <- data.table(data.credit)
# data.cash <- utf82cp936(data.cash)
# data.cash <- data.table(data.cash)

# 计算某产品的三网可发送量

product.name <- enc2utf8('地瓜金融')
black.place.10086 <- enc2utf8(c('北京','重庆'))
black.place.10010 <- enc2utf8(c('北京','福建'))
black.place.10000 <- enc2utf8(c('北京','福建'))

# 手机黑名单
black.phone <- read.table("black_list.txt",col.names = 'phone')
# 通道黑名单
black.channel <- read.csv("channel_bl_Jan.csv",col.names = 'phone')
# 产品已发送
black.repeat <- data.sent.product(product.name = product.name,password = 'Wangxx@2017')
# 7天已发送
black.time7 <- data.sent.7()
# 30天已发送
black.time30 <- data.sent.31()

temp.corp <- function(df){
  a <- subset(df,Corp=='中国移动')
  a <- nrow(subset(a, !Province %in% black.place.10086))
  b <- subset(df,Corp=='中国联通')
  b <- nrow(subset(b, !Province %in% black.place.10010))
  c <- subset(df,Corp=='中国电信')
  c <- nrow(subset(c, !Province %in% black.place.10000))
  d <- a+b+c;
  c(a,b,c,d)
}

temp.f <- function(df){
  
  a <- temp.corp(df)
  df.clean.time <- data.clean(data.clean(df,black.time30),black.time7)
  b <- temp.corp(df.clean.time)
  df.clean.product <- data.clean(df.clean.time,black.repeat)
  c <- temp.corp(df.clean.product)
  output <- data.frame(c)
  rownames(output) <- c('移动','联通','电信','三网');
  output
}

sum_data <- function(df){
  product.name <- enc2utf8(product.name)
  df$date <- as.Date(df$date)
  df <- df[,.(date=max(date)),by=.(phone,业务范围,Province,City,Corp)]
  # 去除黑名单
  df <- subset(df, !phone %in% black.phone$phone)
  df <- subset(df, !phone %in% black.channel$phone)
  df.2017 <- subset(df,date>=as.Date('2016-12-31'))
  df.2016.12 <- subset(df,date>=as.Date('2016-12-01'))
  df.2016.11 <- subset(df,date>=as.Date('2016-11-01'))
  df.2016.10 <- subset(df,date>=as.Date('2016-10-01'))
  
  result <- cbind(temp.f(df.2017),temp.f(df.2016.12),temp.f(df.2016.11),temp.f(df.2016.10))
  colnames(result) <- c('2017','2016.12','2016.11','2016.10');
  result
} 
result.credit <- sum_data(data.credit)
# sum_data(data.cash)
proc.time()-ptm
