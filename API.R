library(data.table)
library(plyr)
library(dplyr)
library(httr)
library(openssl)
library(openxlsx)
library(rjson)
library(rlist)
library(RMySQL)
library(xml2)
library(jsonlite)

api <- 'https://lishu-fd.com/api/imei/phone'
pname <- '201802220001'
# 时间戳参数
ptime <- as.character(round(as.numeric(Sys.time())*1000))
# 用于计算加密
seed <- '76600a674ea4495f9f1a406c82a2ad1d'
# 计算md5加密的vkey
vkey <- md5(paste(seed,ptime,seed,sep='_'))


# 手机号,身份证,姓名,地址 查询query_tag
query.query_tag <- function(phone.vector,idcard.vector,uname.vector,addrs.vector=NA,step=20){
  api <- 'https://lishu-fd.com/api/query_tag'
  pname <- '201802220001'
  
  data.mobiles <- phone.vector
  data.idcards <- idcard.vector
  data.unames <- uname.vector
  if(is.na(addrs.vector)){
    data.addrs <- data.unames
    data.addrs[1:length(data.addrs)] <- ' '
  }
  if(!is.na(addrs.vector)){
    data.addrs <- addrs.vector
  }
  
  query_tag.result <- list()
  query_tag.feedback <- list()
  ptm <- proc.time()
  for (i in seq(1,length(data.mobiles),step)){
    # 时间戳参数
    ptime <- as.character(round(as.numeric(Sys.time())*1000))
    # 用于计算加密
    seed <- '76600a674ea4495f9f1a406c82a2ad1d'
    # 计算md5加密的vkey
    vkey <- md5(paste(seed,ptime,seed,sep='_'))
    mobiles <- toString( data.mobiles[i:min((i+step-1),length(data.mobiles))])
    mobiles <- gsub(" ","",mobiles)
    idcards <- toString( data.idcards[i:min((i+step-1),length(data.idcards))])
    idcards <- gsub(" ","",idcards)
    unames <- toString( data.unames[i:min((i+step-1),length(data.unames))])
    unames <- gsub(" ","",unames)
    addrs <- toString( data.addrs[i:min((i+step-1),length(data.addrs))])
    addrs <- gsub(" ","",addrs)
    tryCatch(rm(response_content),warning =function(w){print(paste(i-1,"response_content不存在"))})
    tryCatch(response_content <- api %>%
               POST(body = list(pname=pname,ptime=ptime,vkey=vkey,unames=unames, mobiles=mobiles,idcards=idcards,addrs=addrs),encode = 'form') %>% 
               content(),error=function(e){print(i)} )
    tryCatch(if(1){
      response_content.temp <- response_content;response_content.temp$timestamp <- Sys.time() %>% toString()
      response_content.json <- toJSON(response_content.temp)
      write(response_content.json,paste0(as.character(Sys.Date()),"query_tag_response_content.json"),append = TRUE)
    },error=function(e){})
    query_tag.result[[floor((i-1)/step+1)]] <- response_content$data
    query_tag.feedback[[floor((i-1)/step+1)]] <- paste(response_content$result,response_content$message)
    
  }
  proc.time() - ptm
  if(FALSE){
    query_tag.list <- list();ii <- 1
    for(i in 1:length(query_tag.result)){
      for (j in 1:length(query_tag.result[[i]])){
        
        a <- query_tag.result[[i]][[j]]
        if(a!=''){
          tag_data.n <- length(a$tag_data)
          a.tag_data <- a$tag_data
          a$tag_data <- NULL
          a$imeis <- a$imeis %>% unlist() %>% toString()
          a$imsis <- a$imsis %>% unlist() %>% toString()
          a$qq <- a$qq %>% unlist() %>% toString()
          a$contact_tel <-  a$contact_tel %>% unlist() %>% toString()
          a$emails <-  a$emails %>% unlist() %>% toString()
          a$contact_addr <-  a$contact_addr %>% unlist() %>% toString()
          a$office_tel <-  a$office_tel %>% unlist() %>% toString()
          a$car_brand <-  a$car_brand %>% unlist() %>% toString()
          
          if(tag_data.n>0){
            tryCatch(a.tag_data$circle_friends <- a.tag_data$circle_friends %>% unlist() %>% toString(),
                     error = function(e){
                       a.tag_data$circle_friends <- ''
                     }
            )
            
            contact_address.n <- length(a.tag_data$contact_address)
            if(contact_address.n>0){
              for (p in 1:contact_address.n){
                a.tag_data$contact_address[[p]]$sender_phone <- a.tag_data$contact_address[[p]]$sender_phone %>% unlist() %>% toString()
                a.tag_data$contact_address[[p]]$recipient_phone <- a.tag_data$contact_address[[p]]$recipient_phone %>% unlist() %>% toString()
              }
              a.tag_data.contact_address <- list.stack(a.tag_data$contact_address)
              a.tag_data$contact_address.sender <- a.tag_data.contact_address$sender %>% toString()
              a.tag_data$contact_address.sender_addr <- a.tag_data.contact_address$send_addr %>% toString()
              a.tag_data$contact_address.sender_phone <- a.tag_data.contact_address$sender_phone %>% toString()
              a.tag_data$contact_address.sender_company <- a.tag_data.contact_address$send_company %>% toString()
              a.tag_data$contact_address.sender_time <- a.tag_data.contact_address$send_time %>% toString()
              a.tag_data$contact_address.recipient <- a.tag_data.contact_address$recipient %>% toString()
              a.tag_data$contact_address.recipient_phone <- a.tag_data.contact_address$recipient_phone %>% toString()
              a.tag_data$contact_address.recipient_addr <- a.tag_data.contact_address$recipient_addr %>% toString()
              a.tag_data$contact_address.recipient_company <- a.tag_data.contact_address$recipient_company %>% toString()
              a.tag_data$contact_address.recipient_idcard <- a.tag_data.contact_address$recipient_idcard %>% toString()
              a.tag_data$contact_address.recipient_date <- a.tag_data.contact_address$recipient_date %>% toString()
            }
            if(contact_address.n==0){
              a.tag_data$contact_address.sender <- ''
              a.tag_data$contact_address.sender_addr <- ''
              a.tag_data$contact_address.sender_phone <- ''
              a.tag_data$contact_address.sender_company <- ''
              a.tag_data$contact_address.sender_time <- ''
              a.tag_data$contact_address.recipient <- ''
              a.tag_data$contact_address.recipient_phone <- ''
              a.tag_data$contact_address.recipient_addr <- ''
              a.tag_data$contact_address.recipient_company <- ''
              a.tag_data$contact_address.recipient_idcard <- ''
              a.tag_data$contact_address.recipient_date <- ''
            }
            white_list.n <- length(a.tag_data$white_list)
            if(white_list.n>0){
              for (q in 1:white_list.n){
                a.tag_data$white_list[[q]]$contact_phone <- a.tag_data$white_list[[q]]$contact_phone %>% unlist() %>% toString()
                a.tag_data$white_list[[q]]$contact_addr <- a.tag_data$white_list[[q]]$contact_addr %>% unlist() %>% toString()
                a.tag_data$white_list[[q]]$affiliated_phone <- a.tag_data$white_list[[q]]$affiliated_phone %>% unlist() %>% toString()
                a.tag_data$white_list[[q]]$affiliated_addr <- a.tag_data$white_list[[q]]$affiliated_addr %>% unlist() %>% toString()
                a.tag_data$white_list[[q]]$affiliated_phoneSet <- a.tag_data$white_list[[q]]$affiliated_phoneSet %>% unlist() %>% toString()
              }
              a.tag_data.white_list <- list.stack(a.tag_data$white_list)
              a.tag_data$white_list.uname <- a.tag_data.white_list$uname %>% toString()
              a.tag_data$white_list.contact_phone <- a.tag_data.white_list$contact_phone %>% toString()
              a.tag_data$white_list.contact_addr <- a.tag_data.white_list$contact_addr %>% toString()
              a.tag_data$white_list.org <- a.tag_data.white_list$org %>% toString()
              a.tag_data$white_list.date_time <- a.tag_data.white_list$date_time %>% toString()
              a.tag_data$white_list.affiliated_person <- a.tag_data.white_list$affiliated_person %>% toString()
              a.tag_data$white_list.affiliated_phone <- a.tag_data.white_list$affiliated_phone %>% toString()
              a.tag_data$white_list.affiliated_addr <- a.tag_data.white_list$affiliated_addr %>% toString()
              a.tag_data$white_list.affiliated_phoneSet <- a.tag_data$white_list.affiliated_phoneSet %>% toString()
            }
            if(white_list.n==0){
              a.tag_data$white_list.uname <- ''
              a.tag_data$white_list.contact_phone <- ''
              a.tag_data$white_list.contact_addr <- ''
              a.tag_data$white_list.org <- ''
              a.tag_data$white_list.date_time <- ''
              a.tag_data$white_list.affiliated_person <- ''
              a.tag_data$white_list.affiliated_phone <- ''
              a.tag_data$white_list.affiliated_addr <- ''
              a.tag_data$white_list.affiliated_phoneSet <- ''
            }
            
          }
          if(tag_data.n==0){
            a.tag_data$circle_friends <- ''
            a.tag_data$contact_address.sender <- ''
            a.tag_data$contact_address.sender_addr <- ''
            a.tag_data$contact_address.sender_phone <- ''
            a.tag_data$contact_address.sender_company <- ''
            a.tag_data$contact_address.sender_time <- ''
            a.tag_data$contact_address.recipient <- ''
            a.tag_data$contact_address.recipient_phone <- ''
            a.tag_data$contact_address.recipient_addr <- ''
            a.tag_data$contact_address.recipient_company <- ''
            a.tag_data$contact_address.recipient_idcard <- ''
            a.tag_data$contact_address.recipient_date <- ''
            a.tag_data$white_list.uname <- ''
            a.tag_data$white_list.contact_phone <- ''
            a.tag_data$white_list.contact_addr <- ''
            a.tag_data$white_list.org <- ''
            a.tag_data$white_list.date_time <- ''
            a.tag_data$white_list.affiliated_person <- ''
            a.tag_data$white_list.affiliated_phone <- ''
            a.tag_data$white_list.affiliated_addr <- ''
            a.tag_data$white_list.affiliated_phoneSet <- ''
          }
          a.tag_data$contact_address <- NULL
          a.tag_data$white_list <- NULL
          a <- c(a,a.tag_data)
          query_tag.list[[ii]] <- a;ii <- ii+1
          fwrite(transform(a),"query_tag查询中.csv",eol = '\r\n',append = TRUE)
        }
        
        
      }
      
    }
    query_tag.table <- list.stack(query_tag.list);
    query_tag.table
  }
  ;
  query_tag.result
  
}

# 处理query_tag的输出结果
data.query_tag.clean <- function(data.query_tag,outputname){
  # 初始化输出结果为一个带有list元素的data.frame
  if(1){
    x <- data.query_tag
    xx <- list()
    for (i in 1:length(x)){xx <- c(xx,x[[i]])}
    x.table.1 <- xx %>% toJSON() %>% jsonlite::fromJSON()
  }
  
  n <- nrow(x.table.1)
  # 基础查询信息5列
  x.table.1.basic <- data.table(x.table.1)[,.(qid,qtime,uname,idcard,phone)]%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
  # 基础信息表
  x.table.1.basic_info <- cbind(x.table.1.basic,x.table.1$basic_info%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table())
  # 网贷风险表
  x.table.1.online_loanrisk <- cbind(x.table.1$online_loan_risk$social_risk,x.table.1$online_loan_risk$blacklist_risk)%>%cbind(x.table.1$online_loan_risk$loan_query_risk)%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
  x.table.1.online_loanrisk <- cbind(x.table.1.basic,x.table.1.online_loanrisk)
  
  
  # 消费习惯表
  x.table.1.consumption_habits.11 <- cbind(x.table.1.basic,x.table.1$consumption_habits[,1:6]%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table())
  x.table.1.consumption_habits.1 <- data.frame(matrix(NA,n,11))
  colnames(x.table.1.consumption_habits.1) <- colnames(x.table.1.consumption_habits.11)
  x.table.1.consumption_habits.1$period <- ''
  for(q in 1:n){
    x.table.1.consumption_habits.1[(q*4-3),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-3),12] <- '一个月内'
    x.table.1.consumption_habits.1[(q*4-2),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-2),12] <-'三个月内'
    x.table.1.consumption_habits.1[(q*4-1),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-1),12] <-'六个月内'
    x.table.1.consumption_habits.1[(q*4-0),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-0),12] <-'十二个月内'
  }
  x.table.1.consumption_habits.2 <- rbind(x.table.1$consumption_habits$debit_card_info$last_1,x.table.1$consumption_habits$debit_card_info$last_3) %>%
    rbind(x.table.1$consumption_habits$debit_card_info$last_6) %>%
    rbind(x.table.1$consumption_habits$debit_card_info$last_12) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
  x.table.1.consumption_habits.3 <- rbind(x.table.1$consumption_habits$credit_card_info$last_1,x.table.1$consumption_habits$credit_card_info$last_3) %>%
    rbind(x.table.1$consumption_habits$credit_card_info$last_6) %>%
    rbind(x.table.1$consumption_habits$credit_card_info$last_12) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
  x.table.1.consumption_habits <- x.table.1.consumption_habits.1
  if(ncol(x.table.1.consumption_habits.2)>0){
    x.table.1.consumption_habits <- x.table.1.consumption_habits%>%
      cbind(x.table.1.consumption_habits.2)
  }
  if(ncol(x.table.1.consumption_habits.3)>0){
    x.table.1.consumption_habits <- x.table.1.consumption_habits%>%
      cbind(x.table.1.consumption_habits.3)
  }
  if(ncol(x.table.1.consumption_habits)==12){
    x.table.1.consumption_habits <- cbind(x.table.1.basic,x.table.1.consumption_habits.1)
  }

  # 联系地址表
  if(class(x.table.1$contact_addrs[[1]])=='list'){x.table.1.contact_addrs <- x.table.1.basic}
  if(class(x.table.1$contact_addrs[[1]])=='data.frame'){
    x.table.1.contact_addrs <- data.frame(qid=NA,qtime=NA,uname=NA,idcard=NA,phone=NA,sender=NA,recipient=NA,sender_phone=NA,send_addr=NA,send_company=NA,send_time=NA,recipient_phone=NA,recipient_addr=NA,recipient_company=NA,recipient_idcard=NA,recipient_date=NA)
    for (j in 1:n){
      if(nrow(x.table.1$contact_addrs[[j]])>0){
        for (jj in 1:nrow(x.table.1$contact_addrs[[j]])){
          temp.temp <- cbind(x.table.1.basic[j,] ,x.table.1$contact_addrs[[j]][jj,]) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
          x.table.1.contact_addrs <- rbind(x.table.1.contact_addrs,temp.temp)
        }
      }
      if(nrow(x.table.1$contact_addrs[[j]])==0){
        temp.temp <- cbind(x.table.1.basic[j,],data.frame(sender='',recipient='',sender_phone='',send_addr='',send_company='',send_time='',recipient_phone='',recipient_addr='',recipient_company='',recipient_idcard='',recipient_date='')) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
        x.table.1.contact_addrs <- rbind(x.table.1.contact_addrs,temp.temp)
      }
      
    }
    x.table.1.contact_addrs <- x.table.1.contact_addrs[-1,] %>% unique()
  }
  
  
  # 朋友圈表
  if(class(x.table.1$circle_friends[[1]])=='list'){x.table.1.circle_friends <- x.table.1.basic}
  if(class(x.table.1$circle_friends[[1]])=='data.frame'){
    x.table.1.circle_friends <- data.frame(qid=NA,qtime=NA,uname=NA,idcard=NA,phone=NA,contact_name=NA,contact_phone=NA)
    for (j in 1:n){
      if(nrow(x.table.1$circle_friends[[j]])>0){
        for (jj in 1:nrow(x.table.1$circle_friends[[j]])){
          temp.temp <- cbind(x.table.1.basic[j,] ,x.table.1$circle_friends[[j]][jj,])%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
        }
      }
      if(nrow(x.table.1$circle_friends[[j]])==0){
        temp.temp <- cbind(x.table.1.basic[j,],data.frame(contact_name='',contact_phone=''))%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
      }
      x.table.1.circle_friends <- rbind(x.table.1.circle_friends,temp.temp)
    }
    x.table.1.circle_friends <- x.table.1.circle_friends[-1,] %>% unique()
    
  }
  
  # 白名单表
  if(class(x.table.1$whitelist[[1]])=='list'){x.table.1.whitelist <- x.table.1.basic}
  if(class(x.table.1$whitelist[[1]])=='data.frame'){
    x.table.1.whitelist <- data.frame(qid=NA,qtime=NA,uname=NA,idcard=NA,phone=NA,uname.1=NA,org=NA,desc='',contact_phone=NA,contact_addr=NA,date_time=NA,affiliated_person=NA,affiliated_phone=NA,affiliated_addr=NA)
    for (j in 1:n){
      if(nrow(x.table.1$whitelist[[j]])>0){
        for (jj in 1:nrow(x.table.1$whitelist[[j]])){
          temp.temp <- cbind(x.table.1.basic[j,] ,x.table.1$whitelist[[j]][jj,]) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table();colnames(temp.temp)[6] <- 'uname.1'
          x.table.1.whitelist <- rbind(x.table.1.whitelist,temp.temp)
        }
      }
      if(nrow(x.table.1$whitelist[[j]])==0){
        temp.temp <- cbind(x.table.1.basic[j,],data.frame(uname.1='',org='',desc='',contact_phone='',contact_addr='',date_time='',affiliated_person='',affiliated_phone='',affiliated_addr=''))%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table() 
        x.table.1.whitelist <- rbind(x.table.1.whitelist,temp.temp)
      }
    }
    x.table.1.whitelist <- x.table.1.whitelist[-1,] %>% unique()
  }
  
  x.table.1.basic_info.model <- read.xlsx("标签报告模板.xlsx",sheet = 1)
  x.table.1.basic_info <- x.table.1.basic_info[,.(qid,qtime,uname,phone,idcard,idcard_validate,idcard_prov,idcard_city,idcard_region,gender,age,record_idcard_days,last_appear_idcard,used_idcards_cnt,idcard_bind_other_name_cnt,phone_operator,phone_prov,phone_city,record_phone_days,last_appear_phone,used_phones_cnt,phone_bind_other_name_cnt,imsis,imeis,emails,contact_tel,contact_addr,office_tel,credit_card_user,exist_online_loan_record,blacklist_user)]
  colnames(x.table.1.basic_info) <- colnames(x.table.1.basic_info.model)
  
  x.table.1.online_loanrisk.model <- read.xlsx("标签报告模板.xlsx",sheet = 2)
  x.table.1.online_loanrisk <- x.table.1.online_loanrisk[,.(qid,qtime,uname,phone,idcard,sn_order1_contacts_cnt,sn_order1_blacklist_contacts_cnt,sn_order2_blacklist_contacts_cnt,sn_order2_blacklist_routers_cnt,sn_order2_blacklist_routers_pct,idcard_in_blacklist,phone_in_blacklist,in_court_blacklist,in_p2p_blacklist,in_bank_blacklist,last_appear_idcard_in_blacklist,last_appear_phone_in_blacklist,search_cnt,org_cnt,search_cnt_recent_7_days,org_cnt_recent_7_days,search_cnt_recent_14_days,org_cnt_recent_14_days,search_cnt_recent_30_days,org_cnt_recent_30_days,search_cnt_recent_60_days,org_cnt_recent_60_days,search_cnt_recent_90_days,org_cnt_recent_90_days,search_cnt_recent_180_days,org_cnt_recent_180_days)]
  colnames(x.table.1.online_loanrisk) <- colnames(x.table.1.online_loanrisk.model)
  
  x.table.1.consumption_habits.model <- read.xlsx("标签报告模板.xlsx",sheet = 3)
  colnames(x.table.1.consumption_habits)[1:12] <- c('查询ID','查询时间','查询姓名','查询身份证号','查询手机号','使用线上消费分期次数','使用线下消费分期次数','使用信用卡代偿次数','使用线上现金贷次数','使用线下现金贷次数','其他','消费周期')
  
  x.table.1.contact_addrs.model <- read.xlsx("标签报告模板.xlsx",sheet = 4)
  x.table.1.contact_addrs <- x.table.1.contact_addrs[,.(qid,qtime,uname,phone,idcard,send_addr,send_company,send_time,recipient,recipient_phone,recipient_addr,recipient_company,recipient_idcard,recipient_date)]
  colnames(x.table.1.contact_addrs) <- colnames(x.table.1.contact_addrs.model)
  colnames(x.table.1.contact_addrs)[8] <- '关联时间';colnames(x.table.1.contact_addrs)[12] <- '关联人企业名称'
  x.table.1.circle_friends.model <- read.xlsx("标签报告模板.xlsx",sheet = 5)
  if(ncol(x.table.1.circle_friends)==7){
    x.table.1.circle_friends <- x.table.1.circle_friends[,.(qid,qtime,uname,phone,idcard,contact_name,contact_phone)]
    colnames(x.table.1.circle_friends) <- colnames(x.table.1.circle_friends.model)
  }
  if(ncol(x.table.1.circle_friends)==5){
    x.table.1.circle_friends <- x.table.1.circle_friends[,.(qid,qtime,uname,phone,idcard)]
    colnames(x.table.1.circle_friends) <- colnames(x.table.1.circle_friends.model)[1:5]
  }
  
  
  x.table.1.whitelist.model <- read.xlsx("标签报告模板.xlsx",sheet = 6)
  x.table.1.whitelist <- x.table.1.whitelist[,.(qid,qtime,uname,phone,idcard,uname.1,contact_phone,contact_addr,org,date_time,affiliated_person,affiliated_phone,affiliated_addr)]
  colnames(x.table.1.whitelist) <- colnames(x.table.1.whitelist.model)
  
  consumption_dict <- read.xlsx("数据验证输出.xlsx",sheet = 4)
  
  wb <- createWorkbook() 
  modifyBaseFont(wb,fontSize = 12)
  addWorksheet(wb,'基本信息')
  addWorksheet(wb,'网贷风险')
  addWorksheet(wb,'消费习惯')
  addWorksheet(wb,"消费习惯字典")
  addWorksheet(wb,'联系地址')
  if(ncol(x.table.1.circle_friends)==7){addWorksheet(wb,'朋友圈')}
  addWorksheet(wb,'白名单')

  writeDataTable(wb,'基本信息',x.table.1.basic_info);freezePane(wb, '基本信息', firstRow = TRUE)
  writeDataTable(wb,'网贷风险',x.table.1.online_loanrisk);freezePane(wb, '网贷风险', firstRow = TRUE)
  writeDataTable(wb,'消费习惯',x.table.1.consumption_habits);freezePane(wb, '消费习惯', firstRow = TRUE)
  writeDataTable(wb,'消费习惯字典',consumption_dict);freezePane(wb,'消费习惯字典', firstRow = TRUE)
  writeDataTable(wb,'联系地址',x.table.1.contact_addrs);freezePane(wb, '联系地址', firstRow = TRUE)
  if(ncol(x.table.1.circle_friends)==7){writeDataTable(wb,'朋友圈',x.table.1.circle_friends);freezePane(wb, '朋友圈', firstRow = TRUE)}
  writeDataTable(wb,'白名单',x.table.1.whitelist);freezePane(wb, '白名单', firstRow = TRUE)

  saveWorkbook(wb,paste0(outputname),overwrite = TRUE)
}

# 处理query_tag和分别用phone,idcard查询car_tag的输出结果
hahaha <- function(data.query_tag,data2.car_tag.phone,data2.car_tag.idcard,outputname){
# 初始化输出结果为一个带有list元素的data.frame
if(1){
  x <- data.query_tag
  xx <- list()
  for (i in 1:length(x)){xx <- c(xx,x[[i]])}
  x.table.1 <- xx %>% toJSON() %>% jsonlite::fromJSON()
}

n <- nrow(x.table.1)
# 基础查询信息5列
x.table.1.basic <- data.table(x.table.1)[,.(qid,qtime,uname,idcard,phone)]%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
# 基础信息表
x.table.1.basic_info <- cbind(x.table.1.basic,x.table.1$basic_info%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table())
# 网贷风险表
x.table.1.online_loanrisk <- cbind(x.table.1$online_loan_risk$social_risk,x.table.1$online_loan_risk$blacklist_risk)%>%cbind(x.table.1$online_loan_risk$loan_query_risk)%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
x.table.1.online_loanrisk <- cbind(x.table.1.basic,x.table.1.online_loanrisk)


# 消费习惯表
x.table.1.consumption_habits.11 <- cbind(x.table.1.basic,x.table.1$consumption_habits[,1:6]%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table())
x.table.1.consumption_habits.1 <- data.frame(matrix(NA,n,11))
colnames(x.table.1.consumption_habits.1) <- colnames(x.table.1.consumption_habits.11)
x.table.1.consumption_habits.1$period <- ''
for(q in 1:n){
  x.table.1.consumption_habits.1[(q*4-3),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-3),12] <- '一个月内'
  x.table.1.consumption_habits.1[(q*4-2),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-2),12] <-'三个月内'
  x.table.1.consumption_habits.1[(q*4-1),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-1),12] <-'六个月内'
  x.table.1.consumption_habits.1[(q*4-0),1:11] <- x.table.1.consumption_habits.11[q,1:11];x.table.1.consumption_habits.1[(q*4-0),12] <-'十二个月内'
}
x.table.1.consumption_habits.2 <- rbind(x.table.1$consumption_habits$debit_card_info$last_1,x.table.1$consumption_habits$debit_card_info$last_3) %>%
  rbind(x.table.1$consumption_habits$debit_card_info$last_6) %>%
  rbind(x.table.1$consumption_habits$debit_card_info$last_12) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
x.table.1.consumption_habits.3 <- rbind(x.table.1$consumption_habits$credit_card_info$last_1,x.table.1$consumption_habits$credit_card_info$last_3) %>%
  rbind(x.table.1$consumption_habits$credit_card_info$last_6) %>%
  rbind(x.table.1$consumption_habits$credit_card_info$last_12) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
if(ncol(x.table.1.consumption_habits.2)>0){
  x.table.1.consumption_habits <- x.table.1.consumption_habits%>%
    cbind(x.table.1.consumption_habits.2)
}
if(ncol(x.table.1.consumption_habits.3)>0){
  x.table.1.consumption_habits <- x.table.1.consumption_habits%>%
    cbind(x.table.1.consumption_habits.3)
}
if(ncol(x.table.1.consumption_habits)==12){
  x.table.1.consumption_habits <- cbind(x.table.1.basic,x.table.1.consumption_habits.1)
}
# 联系地址表
if(class(x.table.1$contact_addrs[[1]])=='list'){x.table.1.contact_addrs <- x.table.1.basic}
if(class(x.table.1$contact_addrs[[1]])=='data.frame'){
  x.table.1.contact_addrs <- data.frame(qid=NA,qtime=NA,uname=NA,idcard=NA,phone=NA,sender=NA,recipient=NA,sender_phone=NA,send_addr=NA,send_company=NA,send_time=NA,recipient_phone=NA,recipient_addr=NA,recipient_company=NA,recipient_idcard=NA,recipient_date=NA)
  for (j in 1:n){
    if(nrow(x.table.1$contact_addrs[[j]])>0){
      for (jj in 1:nrow(x.table.1$contact_addrs[[j]])){
        temp.temp <- cbind(x.table.1.basic[j,] ,x.table.1$contact_addrs[[j]][jj,]) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
        x.table.1.contact_addrs <- rbind(x.table.1.contact_addrs,temp.temp)
      }
    }
    if(nrow(x.table.1$contact_addrs[[j]])==0){
      temp.temp <- cbind(x.table.1.basic[j,],data.frame(sender='',recipient='',sender_phone='',send_addr='',send_company='',send_time='',recipient_phone='',recipient_addr='',recipient_company='',recipient_idcard='',recipient_date='')) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
      x.table.1.contact_addrs <- rbind(x.table.1.contact_addrs,temp.temp)
    }
    
  }
  x.table.1.contact_addrs <- x.table.1.contact_addrs[-1,] %>% unique()
}


# 朋友圈表
if(class(x.table.1$circle_friends[[1]])=='list'){x.table.1.circle_friends <- x.table.1.basic}
if(class(x.table.1$circle_friends[[1]])=='data.frame'){
  x.table.1.circle_friends <- data.frame(qid=NA,qtime=NA,uname=NA,idcard=NA,phone=NA,contact_name=NA,contact_phone=NA)
  for (j in 1:n){
    if(nrow(x.table.1$circle_friends[[j]])>0){
      for (jj in 1:nrow(x.table.1$circle_friends[[j]])){
        temp.temp <- cbind(x.table.1.basic[j,] ,x.table.1$circle_friends[[j]][jj,])%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
      }
    }
    if(nrow(x.table.1$circle_friends[[j]])==0){
      temp.temp <- cbind(x.table.1.basic[j,],data.frame(contact_name='',contact_phone=''))%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table()
    }
    x.table.1.circle_friends <- rbind(x.table.1.circle_friends,temp.temp)
  }
  x.table.1.circle_friends <- x.table.1.circle_friends[-1,] %>% unique()
  
}

# 白名单表
if(class(x.table.1$whitelist[[1]])=='list'){x.table.1.whitelist <- x.table.1.basic}
if(class(x.table.1$whitelist[[1]])=='data.frame'){
  x.table.1.whitelist <- data.frame(qid=NA,qtime=NA,uname=NA,idcard=NA,phone=NA,uname.1=NA,org=NA,desc='',contact_phone=NA,contact_addr=NA,date_time=NA,affiliated_person=NA,affiliated_phone=NA,affiliated_addr=NA)
  for (j in 1:n){
    if(nrow(x.table.1$whitelist[[j]])>0){
      for (jj in 1:nrow(x.table.1$whitelist[[j]])){
        temp.temp <- cbind(x.table.1.basic[j,] ,x.table.1$whitelist[[j]][jj,]) %>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table();colnames(temp.temp)[6] <- 'uname.1'
        x.table.1.whitelist <- rbind(x.table.1.whitelist,temp.temp)
      }
    }
    if(nrow(x.table.1$whitelist[[j]])==0){
      temp.temp <- cbind(x.table.1.basic[j,],data.frame(uname.1='',org='',desc='',contact_phone='',contact_addr='',date_time='',affiliated_person='',affiliated_phone='',affiliated_addr=''))%>%lapply(as.character)%>%data.frame(stringsAsFactors = FALSE)%>%data.table() 
      x.table.1.whitelist <- rbind(x.table.1.whitelist,temp.temp)
    }
  }
  x.table.1.whitelist <- x.table.1.whitelist[-1,] %>% unique()
}

x.table.1.basic_info.model <- read.xlsx("标签报告模板.xlsx",sheet = 1)
x.table.1.basic_info <- x.table.1.basic_info[,.(qid,qtime,uname,phone,idcard,idcard_validate,idcard_prov,idcard_city,idcard_region,gender,age,record_idcard_days,last_appear_idcard,used_idcards_cnt,idcard_bind_other_name_cnt,phone_operator,phone_prov,phone_city,record_phone_days,last_appear_phone,used_phones_cnt,phone_bind_other_name_cnt,imsis,imeis,emails,contact_tel,contact_addr,office_tel,credit_card_user,exist_online_loan_record,blacklist_user)]
colnames(x.table.1.basic_info) <- colnames(x.table.1.basic_info.model)

x.table.1.online_loanrisk.model <- read.xlsx("标签报告模板.xlsx",sheet = 2)
x.table.1.online_loanrisk <- x.table.1.online_loanrisk[,.(qid,qtime,uname,phone,idcard,sn_order1_contacts_cnt,sn_order1_blacklist_contacts_cnt,sn_order2_blacklist_contacts_cnt,sn_order2_blacklist_routers_cnt,sn_order2_blacklist_routers_pct,idcard_in_blacklist,phone_in_blacklist,in_court_blacklist,in_p2p_blacklist,in_bank_blacklist,last_appear_idcard_in_blacklist,last_appear_phone_in_blacklist,search_cnt,org_cnt,search_cnt_recent_7_days,org_cnt_recent_7_days,search_cnt_recent_14_days,org_cnt_recent_14_days,search_cnt_recent_30_days,org_cnt_recent_30_days,search_cnt_recent_60_days,org_cnt_recent_60_days,search_cnt_recent_90_days,org_cnt_recent_90_days,search_cnt_recent_180_days,org_cnt_recent_180_days)]
colnames(x.table.1.online_loanrisk) <- colnames(x.table.1.online_loanrisk.model)

x.table.1.consumption_habits.model <- read.xlsx("标签报告模板.xlsx",sheet = 3)
colnames(x.table.1.consumption_habits)[1:12] <- c('查询ID','查询时间','查询姓名','查询身份证号','查询手机号','使用线上消费分期次数','使用线下消费分期次数','使用信用卡代偿次数','使用线上现金贷次数','使用线下现金贷次数','其他','消费周期')

x.table.1.contact_addrs.model <- read.xlsx("标签报告模板.xlsx",sheet = 4)
x.table.1.contact_addrs <- x.table.1.contact_addrs[,.(qid,qtime,uname,phone,idcard,send_addr,send_company,send_time,recipient,recipient_phone,recipient_addr,recipient_company,recipient_idcard,recipient_date)]
colnames(x.table.1.contact_addrs) <- colnames(x.table.1.contact_addrs.model)
colnames(x.table.1.contact_addrs)[8] <- '关联时间';colnames(x.table.1.contact_addrs)[12] <- '关联人企业名称'
x.table.1.circle_friends.model <- read.xlsx("标签报告模板.xlsx",sheet = 5)
if(ncol(x.table.1.circle_friends)==7){
  x.table.1.circle_friends <- x.table.1.circle_friends[,.(qid,qtime,uname,phone,idcard,contact_name,contact_phone)]
  colnames(x.table.1.circle_friends) <- colnames(x.table.1.circle_friends.model)
}
if(ncol(x.table.1.circle_friends)==5){
  x.table.1.circle_friends <- x.table.1.circle_friends[,.(qid,qtime,uname,phone,idcard)]
  colnames(x.table.1.circle_friends) <- colnames(x.table.1.circle_friends.model)[1:5]
}


x.table.1.whitelist.model <- read.xlsx("标签报告模板.xlsx",sheet = 6)
x.table.1.whitelist <- x.table.1.whitelist[,.(qid,qtime,uname,phone,idcard,uname.1,contact_phone,contact_addr,org,date_time,affiliated_person,affiliated_phone,affiliated_addr)]
colnames(x.table.1.whitelist) <- colnames(x.table.1.whitelist.model)

consumption_dict <- read.xlsx("数据验证输出.xlsx",sheet = 4)

data2.car_tag <- funion(data.table(data2.car_tag.phone),data.table(data2.car_tag.idcard))
data2.car_tag.idcard <- merge(x.table.1.basic,data2.car_tag,by.x = 'idcard',by.y = 'idcard') 
data2.car_tag.phone <- merge(x.table.1.basic,data2.car_tag,by.x = 'phone',by.y = 'phone')


data2.car_tag <- funion(data2.car_tag.idcard[,.(查询姓名=uname,查询手机号=phone.x,查询身份证号=idcard,车型=model,品牌=brand,牌照国家=country,牌照省份=prov,牌照城市=city,车牌号=car_num,注册时间=reg_time)],
                       data2.car_tag.phone[,.(查询姓名=uname,查询手机号=phone,查询身份证号=idcard.x,车型=model,品牌=brand,牌照国家=country,牌照省份=prov,牌照城市=city,车牌号=car_num,注册时间=reg_time)]) %>% unique()
data2.car_tag <- merge(x.table.1.basic,
                       data2.car_tag,
                       by.x = 'phone',by.y = '查询手机号',all.x = TRUE)%>%unique()
data2.car_tag <- data2.car_tag[,.(查询姓名=uname,查询手机号=phone,查询身份证号=idcard,车型,品牌,牌照国家,牌照省份,牌照城市,车牌号,注册时间)]


wb <- createWorkbook() 
modifyBaseFont(wb,fontSize = 12)
addWorksheet(wb,'基本信息')
addWorksheet(wb,'网贷风险')
addWorksheet(wb,'消费习惯')
addWorksheet(wb,"消费习惯字典")
addWorksheet(wb,'联系地址')
if(ncol(x.table.1.circle_friends)==7){addWorksheet(wb,'朋友圈')}
addWorksheet(wb,'白名单')
addWorksheet(wb,'车辆信息')


writeDataTable(wb,'基本信息',x.table.1.basic_info);freezePane(wb, '基本信息', firstRow = TRUE)
writeDataTable(wb,'网贷风险',x.table.1.online_loanrisk);freezePane(wb, '网贷风险', firstRow = TRUE)
writeDataTable(wb,'消费习惯',x.table.1.consumption_habits);freezePane(wb, '消费习惯', firstRow = TRUE)
writeDataTable(wb,'消费习惯字典',consumption_dict);freezePane(wb,'消费习惯字典', firstRow = TRUE)
writeDataTable(wb,'联系地址',x.table.1.contact_addrs);freezePane(wb, '联系地址', firstRow = TRUE)
if(ncol(x.table.1.circle_friends)==7){writeDataTable(wb,'朋友圈',x.table.1.circle_friends);freezePane(wb, '朋友圈', firstRow = TRUE)}
writeDataTable(wb,'白名单',x.table.1.whitelist);freezePane(wb, '白名单', firstRow = TRUE)
writeDataTable(wb,'车辆信息',data2.car_tag);freezePane(wb, '车辆信息', firstRow = TRUE)
saveWorkbook(wb,paste0(outputname),overwrite = TRUE)
}


#用手机号查询car_tag

query.car_tag.phone <- function(phone.vector,step=100){
  pname <- '201802220001'
  seed <- '76600a674ea4495f9f1a406c82a2ad1d'
  api <- 'https://lishu-fd.com/api/car_tag/mobile'
  
  phone.data <- phone.vector
  
  car.result <- data.frame()
  result <-list()
  for (i in seq(1,length(phone.data),step)){
    # 时间戳参数
    ptime <- as.character(round(as.numeric(Sys.time())*1000))
    # 计算md5加密的vkey
    vkey <- md5(paste(seed,ptime,seed,sep='_'))
    phone <- toString(phone.data[(i+0):min((step-1),length(phone.data))])
    phone <- gsub(" ","",phone)
    tryCatch(rm(response_content),warning =function(w){print(paste(i-1,"response_content不存在"))})
    tryCatch(response_content <- api %>%
               POST(body = list(pname=pname,ptime=ptime,vkey=vkey,mobiles=phone),encode = 'form') %>% 
               content(),error=function(e){print(i,'查询失败')} )
    tryCatch(if(1){response_content.temp <- response_content;response_content.temp$timestamp <- Sys.time() %>% toString()
    response_content.json <- toJSON(response_content.temp)
    write(response_content.json,paste0(as.character(Sys.Date()),"car_tag_phone_response_content.json"),append = TRUE)
    },error=function(e){})
    tryCatch(if(1){
      car.result.table <- list.stack(response_content$data)
      car.result <- rbind(car.result.table,car.result)
    },error=function(e){print(paste(i,'好像没东西'))})
    
    tryCatch(
      result[[floor((i-1)/step)+1]] <- response_content$message
      ,error=function(e){print(i)} )
  };
  car.result
  
}


#用身份证查询car_tag

query.car_tag.idcard <- function(idcard.vector,step=100){
  
  pname <- '201802220001'
  seed <- '76600a674ea4495f9f1a406c82a2ad1d'
  api <- 'https://lishu-fd.com/api/car_tag/idcard'
  
  idcard.data <- idcard.vector
  
  car.result <- data.frame()
  for (i in seq(1,length(idcard.data),step)){
    # 时间戳参数
    ptime <- as.character(round(as.numeric(Sys.time())*1000))
    # 计算md5加密的vkey
    vkey <- md5(paste(seed,ptime,seed,sep='_'))
    idcard <- toString(idcard.data[(i+0):min((i+step-1),length(idcard.data))])
    idcard <- gsub(" ","",idcard)
    idcard.table <- data.table(idcard);colnames(idcard.table) <- 'idcard'
    tryCatch(rm(response_content),warning =function(w){print(paste(i-1,"response_content不存在"))})
    tryCatch(response_content <- api %>%
               POST(body = list(pname=pname,ptime=ptime,vkey=vkey,idcards=idcard),encode = 'form') %>% 
               content(),error=function(e){} )
    tryCatch(if(1){                  response_content.temp <- response_content;response_content.temp$timestamp <- Sys.time() %>% toString()
    response_content.json <- toJSON(response_content.temp)
    write(response_content.json,paste0(as.character(Sys.Date()),"car_tag_idcard_response_content.json"),append = TRUE)
    },error=function(e){})
    tryCatch(if(1){
      car.result.table <- list.stack(response_content$data)
      car.result <- rbind(car.result.table,car.result)
    },error=function(e){print(paste(i,'好像没东西'))})
    
    tryCatch(
      result[[floor((i-1)/step)+1]] <- response_content$message
      ,error=function(e){print(i)} )
  };
  car.result
}


#用身份证查询用户信息

query.user.idcard <- function(idcard.vector,step=100){
  
  pname <- '201802220001'
  seed <- '76600a674ea4495f9f1a406c82a2ad1d'
  api <- 'https://lishu-fd.com/api/user/idcard'
  
  idcard.data <- idcard.vector
  
  user.result <- data.frame()
  for (i in seq(1,length(idcard.data),step)){
    # 时间戳参数
    ptime <- as.character(round(as.numeric(Sys.time())*1000))
    # 计算md5加密的vkey
    vkey <- md5(paste(seed,ptime,seed,sep='_'))
    idcard <- toString(idcard.data[(i+0):min((i+step-1),length(idcard.data))])
    idcard <- gsub(" ","",idcard)
    idcard.table <- data.table(idcard);colnames(idcard.table) <- 'idcard'
    tryCatch(rm(response_content),warning =function(w){print(paste(i-1,"response_content不存在"))})
    tryCatch(response_content <- api %>%
               POST(body = list(pname=pname,ptime=ptime,vkey=vkey,idcards=idcard),encode = 'form') %>% 
               content(),error=function(e){} )
    tryCatch(if(1){                  response_content.temp <- response_content;response_content.temp$timestamp <- Sys.time() %>% toString()
    response_content.json <- toJSON(response_content.temp)
    write(response_content.json,paste0(as.character(Sys.Date()),"user_idcard_response_content.json"),append = TRUE)
    },error=function(e){})
    tryCatch(if(1){
      user.result.table <- list.stack(response_content$data)
      user.result <- rbind(user.result.table,user.result)
    },error=function(e){print(paste(i,'好像没东西'))})
    
    tryCatch(
      result[[floor((i-1)/step)+1]] <- response_content$message
      ,error=function(e){print(i)} )
    
  };
  user.result
}


#用手机号查询用户信息

query.user.phone <- function(phone.vector,step=100){
  
  pname <- '201802220001'
  seed <- '76600a674ea4495f9f1a406c82a2ad1d'
  api <- 'https://lishu-fd.com/api/user/mobile'
  
  phone.data <- phone.vector
  
  user.result <- data.frame()
  for (i in seq(1,length(phone.data),step)){
    # 时间戳参数
    ptime <- as.character(round(as.numeric(Sys.time())*1000))
    # 计算md5加密的vkey
    vkey <- md5(paste(seed,ptime,seed,sep='_'))
    phone <- toString(phone.data[(i+0):min((i+step-1),length(phone.data))])
    phone <- gsub(" ","",phone)
    tryCatch(rm(response_content),warning =function(w){print(paste(i-1,"response_content不存在"))})
    tryCatch(response_content <- api %>%
               POST(body = list(pname=pname,ptime=ptime,vkey=vkey,mobiles=phone),encode = 'form') %>% 
               content(),error=function(e){} )
    tryCatch(if(1){                  response_content.temp <- response_content;response_content.temp$timestamp <- Sys.time() %>% toString()
    response_content.json <- toJSON(response_content.temp)
    write(response_content.json,paste0(as.character(Sys.Date()),"user_phone_response_content.json"),append = TRUE)
    },error=function(e){})
    tryCatch(if(1){
      user.result.table <- list.stack(response_content$data)
      user.result <- rbind(user.result.table,user.result)
    },error=function(e){print(paste(i,'好像没东西'))})
    
    tryCatch(
      if(1){result[[floor((i-1)/step)+1]] <- response_content$message}
      ,error=function(e){print(i)} )  
  };
  user.result
}

