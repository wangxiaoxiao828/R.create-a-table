library(RMySQL)
library(plyr)
library(openxlsx)
library(data.table)
source("feature.R",encoding = 'UTF-8')
source("con_mysql.R",encoding = 'UTF-8')
ptm <- proc.time()



#data.total当日所有下发数据
data.total <- data.frame()
#读取下发计划
product <- read.xlsx("C:/Users/xx/Documents/下发计划.xlsx", detectDates = TRUE)
#下发时间
cur.date <- as.Date(product[1,5])
 
#黑号记录
black.phone <- read.table("black_list.txt",col.names = 'phone')
black.channel <- read.csv("channel_bl_Jan.csv",col.names = 'phone')

for (iii in 1:nrow(product)) {
#第iii行产品屏蔽地点，产品名，通道，发送数量
black.place <- product[iii,2]
black.place <- gsub("，",",",black.place)
black.place <- gsub(",","','",black.place)
black.place <- gsub(" ","",black.place)
operator <- product[iii,4]
product.name <- product[iii,1]
product.channel <- product[iii,6]
product.number <- as.numeric(product[iii,3])
data.total <- data.frame()
#提取产品已发送号码
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

#读取随时现金记录
 con <- dbConnect(MySQL(),host="192.168.0.20",dbname="db_traffic",user="wangxx",password="Wangxx@2017")
 res <- dbSendQuery(con, "select phone from tb_anytimecash_agent;")
 black.suishixianjin <- dbFetch(res, n=-1)
 dbDisconnect(con)
#NNN已获取数据条数


data.fetched <- data.frame()



con <- dbConnect(MySQL(),host="192.168.0.20",dbname="db_traffic",user="wangxx",password="Wangxx@2017")

#res <- dbSendQuery(con, paste("SELECT distinct phone FROM send_down_xianjindai where city not in ('",black.place,"') and corp = '",operator,"'",sep =""))
res <- dbSendQuery(con, paste("SELECT distinct phone FROM send_down_credit where city not in ('",black.place,"') and corp = '",operator,"'",sep =""))

data <- dbFetch(res, n=-1)

dbDisconnect(con)

#去重

data <- unique(data)

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
data <- subset(data,!not.30day)

#去掉随时现金
 not.suishixianjin <- data$phone %in% black.suishixianjin$phone
 data <- subset(data,!not.suishixianjin)

data.fetched <- rbind(data.fetched,data)
data.fetched <- unique(data.fetched)





data.fetched <- transform(sample(data.fetched$phone,product.number))
#data.fetched <- transform(data.fetched[1:product.number,],stringsAsFactors= FALSE)
write.xlsx(data.fetched,paste(product.name,"_",cur.date,"_",product.channel,"_",operator,"_",product.number,".xlsx",sep=""),col.names = FALSE)
#write.table(data.fetched,paste(product.name,cur.date,"_",product.number,".txt",sep=""),row.names = FALSE,col.names = FALSE)
#paste(product.name,cur.date,"_",product.number,".csv",sep="")

data.fetched$product <- product.name
data.fetched$send_down_date <- cur.date
data.fetched$channel <- product.channel
data.total <- rbind(data.total,data.fetched)

write.csv(data.total,paste("data_sent_",product.name,cur.date,product.channel,product.number,".csv",sep=""),row.names = FALSE,fileEncoding = 'UTF-8')
###上传已发数据
con <- dbConnect(MySQL(),host="192.168.0.20",dbname="db_traffic",user="wangxx",password="Wangxx@2017")

res <- dbSendQuery(con, paste("load data local infile 'C:/Users/xx/Documents/data_sent_", product.name,cur.date,product.channel, product.number,".csv' IGNORE into table tb_send_down_record
                   FIELDS
                   ENCLOSED BY '\"'
                   terminated by ','
                   LINES TERMINATED BY '\r\n'
                   IGNORE 1 lines
                   (phone, product, send_down_date,channel);",sep=""))
data.credit <- dbFetch(res, n=-1)

dbDisconnect(con)
}
proc.time()-ptm
data.fetched <- transform(data$phone)
