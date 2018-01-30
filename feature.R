# 标记三网.归属地省市
# 文件为只有单列手机号
# file.name <- '光大银行_2018-01-22_20000.xlsx'
# dis.operator区分三网输出xlsx文件
# check.city标记手机号归属地及三网
# dis.operator(file.name='光大银行_2018-01-22_20000.xlsx')
# data <- check.city(data=data,phone='phone')

library(openxlsx)
library(data.table)
feature.help <- function(){
  print("# 标记三网.归属地省市
        # 文件为只有单列手机号
        # file.name <- '光大银行_2018-01-22_20000.xlsx'
        # dis.operator区分三网输出xlsx文件
        # check.city标记手机号归属地及三网
        # dis.operator(file.name='光大银行_2018-01-22_20000.xlsx')
        # data <- check.city(data=data,phone='phone')")
}

dis.operator <- function(file.name) {
phone.data <- read.xlsx(paste(file.name),colNames = FALSE)

colnames(phone.data) <- c("phone")
  j.operator <- function(p) {
    operator <- ifelse(grepl("^(133|149|153|18[019]|17[237]|199)",p),'10000',"")
    operator <- ifelse(grepl("^(13[012]|14[56]|15[56]|166|17[01]|17[56]|18[56])",p),'10010',operator)
    operator <- ifelse(grepl("^(13[4-9]|14[78]|15([0-2]|[7-9])|178|18([2-4]|[78])|198)",p),'10086',operator);
    operator
    
  }

  phone.data$operator <- j.operator(phone.data$phone)
  
  phone.10010 <- subset(phone.data,phone.data$operator=='10010')
  phone.10010 <- phone.10010[,1]
  write.xlsx(phone.10010,file=paste("10010",file.name,sep=""),col.names=FALSE,row.names=FALSE)


  phone.10086 <- subset(phone.data,phone.data$operator=='10086')
  phone.10086 <- phone.10086[,1]
  write.xlsx(phone.10086,file=paste("10086",file.name,sep=""),col.names=FALSE,row.names=FALSE)

  phone.10000 <- subset(phone.data,phone.data$operator=='10000')
  phone.10000 <- phone.10000[,1]
  write.xlsx(phone.10000,file=paste("10000",file.name,sep=""),col.names=FALSE,row.names=FALSE)
}

dbo_mobile <- fread("dbo_mobile.csv",integer64 = 'character',encoding = 'UTF-8')

check.city <- function(data,phone='phone'){
  data$p7 <- substring(data[,c(phone)],1,7)
  data <- merge(data,dbo_mobile,by.x = 'p7',by.y = 'Mobile')
  data$p7 <- NULL;
  data
}
