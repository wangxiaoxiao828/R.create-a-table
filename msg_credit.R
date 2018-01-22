#提取信用卡数据
#来源message2中 所有符合信用卡标签
#信用卡2未用.csv
#2018.1.22
library(RMySQL)
library(data.table)
library(openxlsx)

source("C:/Users/xx/Documents/dis_operator.R")

#提取参数
product.name <- "光大银行"
product.number <- 20000
cur.date <- as.Date("2018-01-22")
product.channel <- ""

#提取
data <- fread("信用卡分期1221Fmsg2.txt",encoding="UTF-8")
colnames(data) <- 'phone'
#目标地区
cities <- read.table("C:/Users/xx/Downloads/光大银行列表.txt",encoding="UTF-8")

#手机号归属地文件
dbo_mobile <- fread("dbo_Mobile.csv",encoding='UTF-8')
#取手机前7位
data$p7 <- substring(data$phone,1,7)
dbo_mobile$Mobile <- as.character(dbo_mobile$Mobile)

data <- merge(data,dbo_mobile,by.x = 'p7',by.y = 'Mobile')
#所有符合归属地条件,运营商号码
data <- subset(data,Corp %in% c("中国移动","中国联通"))
data <- subset(data,City!='北京');data <- subset(data,City!='重庆')

data.ok <- subset(data,data$City %in% cities$V1)

data.clean <- function(data){     
  ##黑号记录
  black.phone <- read.table("black_list.txt",col.names = 'phone')
  black.channel <- read.csv("channel_bl_Jan.csv",col.names = 'phone')
  con <- dbConnect(MySQL(),host="192.168.0.20",dbname="db_traffic",user="wangxx",password="Wangxx@2017")
  res <- dbSendQuery(con, paste("select phone from tb_send_down_record where product = '",product.name,"';",sep=""))
  black.repeat <- dbFetch(res, n=-1)
  dbDisconnect(con)
  #读取7天内发送记录
  con <- dbConnect(MySQL(),host="192.168.0.20",dbname="db_traffic",user="wangxx",password="Wangxx@2017")
  res <- dbSendQuery(con, paste("select phone from tb_send_down_record where send_down_date > '",cur.date-8,"';",sep=""))
  black.time7 <- dbFetch(res, n=-1)
  #读取30天内发送记录
  res <- dbSendQuery(con, paste("select phone,count(1) from tb_send_down_record where send_down_date > '",cur.date-32,"' group by phone;",sep=""))
  black.time30 <- dbFetch(res, n=-1)
  dbDisconnect(con)
  black.time7 <- unique(black.time7)
  #30天发送3次号码
  black.time30 <- subset(black.time30, `count(1)`>=3, phone)
  # 去掉黑号
  not.black <- data$phone %in% black.phone$phone
  data <-  subset(data,!not.black)
  # 去掉通道黑号
  not.channel <- data$phone %in% black.channel$phone
  data <- subset(data,!not.channel)
  #去掉产品重复
  not.repeat <- data$phone %in% black.repeat$phone
  data <- subset(data,!not.repeat)
  #去掉7天内重复
  not.7day <- data$phone %in% black.time7$phone
  data <- subset(data,!not.7day)
  #去掉30天内重复
  not.30day <- data$phone %in% black.time30$phone
  data <- subset(data,!not.30day);
  data
}   
data.ok <- data.clean(data.ok)

data.random <- sample(data.ok$phone,product.number)
write.xlsx(data.random,paste(product.name,"_",cur.date,"_",product.number,".xlsx",sep = ""),row.names=FALSE)
dis_operator(paste(product.name,"_",cur.date,"_",product.number,".xlsx",sep = ""))

data.random <- transform(data.random)
data.random$product <- product.name
data.random$send_down_date <- as.POSIXct(strptime(paste(cur.date,"00:00:01"), "%Y-%m-%d %H:%M:%S"))
data.random$channel <- product.channel
write.csv(data.random,paste("data_sent_",product.name,"_",cur.date,"_",product.number,"_",product.channel,".csv",sep=""),row.names = FALSE,fileEncoding = 'UTF-8')

  ###上传已发数据
  con <- dbConnect(MySQL(),host="192.168.0.20",dbname="db_traffic",user="wangxx",password="Wangxx@2017")
  res <- dbSendQuery(con, paste("load data local infile 'C:/Users/xx/Documents/data_sent_", iconv(product.name,"CP936","UTF-8"),"_",cur.date,"_",product.number,"_",product.channel, ".csv' IGNORE into table tb_send_down_record
                     FIELDS
                     ENCLOSED BY '\"'
                     terminated by ','
                     LINES TERMINATED BY '\r\n'
                     IGNORE 1 lines
                     (phone, product, send_down_date,channel);",sep=""))
  data.credit <- dbFetch(res, n=-1)
  dbDisconnect(con)
  write.table(paste("data_sent_",product.name,"_",cur.date,"_",product.number,"_",product.channel,".csv",sep=""),file="D:/每日发送/tb_send_down_record.txt",append=TRUE,sep=",",row.names=FALSE)
