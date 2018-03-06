# 调用接口查询imei
# IMEI 至少要求前 14 位，不能超过 16 位的纯数字。返回的 imei 全部为 14 位
# 可以多个 imei 逗号分 隔，最多一次支持 100 条
# 参数为pname,ptime,vkey,imei
# response_content <- api %>%
#   POST(body = list(pname=pname,ptime=ptime,vkey=vkey,imei=imei),encode = 'form') %>% 
#   content()
library(data.table)
library(dplyr)
library(httr)
library(openssl)
library(openxlsx)
library(rjson)
library(tcltk)
imei.data <- read.xlsx("C:/Users/xx/Downloads/10w_imei.xlsx")
temp <- fread("sample_imei1w.csv")
imei.data <- subset(imei.data, !IMEI %in% temp$x)
imei.data <- subset(imei.data, grepl("\\d{14}",imei.data$IMEI))

api <- 'https://lishu-fd.com/api/imei/phone'
pname <- '201802220001'
# 时间戳参数
ptime <- as.character(round(as.numeric(Sys.time())*1000))
# 用于计算加密
seed <- '76600a674ea4495f9f1a406c82a2ad1d'
# 计算md5加密的vkey
vkey <- md5(paste(seed,ptime,seed,sep='_'))
# 带查询imei
imei <- '351551071284514,351551071335746'


imei.data <- imei.data$IMEI
write.csv(imei.data,"sample_imei.csv",row.names = FALSE)
result <-list()
ptm <- Sys.time()
#开启进度条  
pb <- tkProgressBar("进度","已完成 %", 0, 100)

for (i in seq(1,length(imei.data),100)){
  # 时间戳参数
  ptime <- as.character(round(as.numeric(Sys.time())*1000))
  # 计算md5加密的vkey
  vkey <- md5(paste(seed,ptime,seed,sep='_'))
  imei <- toString(imei.data[i:min((i+99),length(imei.data))])
  imei <- gsub(" ","",imei)
  # response_content <- api %>%
  #   POST(body = list(pname=pname,ptime=ptime,vkey=vkey,imei=imei),encode = 'form') %>% 
  #   content()
  
  tryCatch(response_content <- api %>%
             POST(body = list(pname=pname,ptime=ptime,vkey=vkey,imei=imei),encode = 'form') %>% 
             content(),error=function(e){print(i)} )
  # result[[i]] <- response_content$data
  tryCatch(
    if(response_content$result %in% c('200','204')){
      result[[i]] <- response_content$data
    }else{result[[i]] <- response_content$message}
    ,error=function(e){print(i)} )
  
  info <- sprintf("已完成 %d%%", round(i*100/length(imei.data)))
  setTkProgressBar(pb, i*100/length(imei.data), sprintf("进度 (%s)", info),info)

}
d <- unlist(result)
d <- data.table(d)
fwrite(d,"d.csv",append = TRUE)
#关闭进度条  
close(pb) 
Sys.time()-ptm
