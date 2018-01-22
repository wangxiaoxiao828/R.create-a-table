#seperate 3 operator

library(openxlsx)

#file.name <- '光大银行_2018-01-22_20000.xlsx'
dis_operator <- function(file.name) {
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
#write.table(phone.10010,file="贷上我10010_2018-01-19.txt",col.names=FALSE,row.names=FALSE)
write.xlsx(phone.10010,file=paste("10010",file.name,sep=""),col.names=FALSE,row.names=FALSE)


phone.10086 <- subset(phone.data,phone.data$operator=='10086')
phone.10086 <- phone.10086[,1]
#write.table(phone.10086,file="贷上我10086_2018-01-19.txt",col.names=FALSE,row.names=FALSE)
write.xlsx(phone.10086,file=paste("10086",file.name,sep=""),col.names=FALSE,row.names=FALSE)

phone.10000 <- subset(phone.data,phone.data$operator=='10000')
phone.10000 <- phone.10000[,1]
#write.table(phone.10000,file="贷上我10000_2018-01-19.txt",col.names=FALSE,row.names=FALSE)
write.xlsx(phone.10000,file=paste("10000",file.name,sep=""),col.names=FALSE,row.names=FALSE)
}

