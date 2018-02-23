# 专为匹配sms_data中的imei_x
# 与imei_x2phone_xx.csv相比, 此文件包含所有对应关系<=3的imei_x和phone
# 从6个文件夹中读取共60G文件, 1. 取出其中imei_x-phone部分;
# 2. 取出其中imei_o-imei_x部分,并与tb_phone_imei中去除欺诈的imei_o-phone进行merge, 得到imei_x-phone
# 基础: 待匹配的sms_data中为imsi_x,imei_x 故最终需要imei_x-phone. 其由两部分构成: 
# 一. identify中的imei_x-phone 二. 由identify中的imei_o-imei_x 匹配tb_phone_imei中的imei_o-phone得到


library(data.table)
library(bit64)
library(RMySQL)
source("feature.R")

temp.function <- function(path){
  file.remove("temp.csv")
  
  # 写下该文件夹下所有文件
  file.list <- list.files(path)
  for(i in 1:length(file.list)){
    temp <- fread(paste(path,file.list[i],sep=""),header = FALSE, colClasses = c('character','character','character','character','character','character'))
    temp <- temp[,.(V3,V4,V5)]
    colnames(temp) <- c('imei_o','imei_x','phone')
    fwrite(temp,'temp.csv',eol='\r\n',append = TRUE)
  }
  
  temp <- fread("temp.csv",colClasses = c('character','character','character'),header=TRUE)
  temp <- unique(temp)
  # 取出imei_x-phone部分
  temp.phone <- subset(temp,phone!='\\N')
  temp.phone <- temp.phone[,.(imei_x,phone)]
  temp.phone <- unique(temp.phone)
  temp.phone <- check.city(temp.phone)
  fwrite(temp.phone,paste(substring(path,nchar(path)-13,nchar(path)-1),'_a',sep = ''),eol='\r\n',append = TRUE)
  
  # # 计算imei对应关系数
  # imei_o.n <- temp.phone[,.(n=length(phone)),by=.(imei_o)]
  # temp.fraud.imei <- as.data.frame(with(subset(imei_o.n,n>=4),imei_o),stringsAsFactors=FALSE)
  # colnames(temp.fraud.imei) <- 'imei'
  # fwrite(temp.fraud.imei,"temp_fraud_imei.csv",eol='\r\n',append = TRUE)
  # 
  # # 计算phone对应关系数
  # phone.n <- temp.phone[,.(n=length(imei_o)),by=.(phone)]
  # temp.fraud.phone <- as.data.frame(with(subset(phone.n,n>=4),phone),stringsAsFactors = FALSE)
  # colnames(temp.fraud.phone) <- 'phone'
  # fwrite(temp.fraud.phone,"temp_fraud_phone.csv",eol = '\r\n',append = TRUE)
  # 
  # # 去除欺诈imei和phone
  # temp.phone <- subset(temp.phone,!imei_o %in% temp.fraud.imei$imei)
  # temp.phone <- subset(temp.phone,!phone %in% temp.fraud.phone$phone)
  
  # 取出所有imei_o-imei_x部分
  temp <- temp[,.(imei_o,imei_x)]
  # 与dict_data/tb_phone_imei中的imei_o-phone进行匹配
  dict.list <- list.files("tb_phone_imei/")
  
  for (j in 1:length(dict.list)){
    dict.imei <- fread(paste("tb_phone_imei/",dict.list[j],sep=""),header = TRUE, colClasses = c("bit64","character"))
    x <- merge(temp,dict.imei, by.x='imei_o',by.y='imei')
    x <- x[,.(imei_x,phone)]
    x <- unique(x)
    fwrite(x,paste(substring(path,nchar(path)-13,nchar(path)-1),'_b',sep = ''),eol='\r\n',append = TRUE)
  }
}
temp.function(path = "/data/identify/identify_end_20170625/")
temp.function(path = "/data/identify/id_2017_03_06/")
temp.function(path = "/data/identify/id_201611_201702/")
temp.function(path = "/data/identify/id_201607_201610/")
temp.function(path = "/homw/wangxx/dict_data/id_201510_201602/")
temp.function(path = "/homw/wangxx/dict_data/id_201603_201606/")
