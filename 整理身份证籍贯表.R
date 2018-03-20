# 重制根据身份证判断籍贯的表
# 重制整合tb_id_city_old_policy和tb_id_city_new_policy
# 输出为scan_id,province,city
# tb_id_city_old_policy中籍贯为省市合在同一字段下

library(data.table)
library(dplyr)
library(plyr)

id.old <- fread("E:/tb_id_city_old_policy.csv",encoding = 'UTF-8')
id.new <- fread("E:/tb_id_city_new_policy.csv",encoding = 'UTF-8')

id.province <- id.new[,.(province)] %>% unique()
id.old[649,2] <- '黑龙江齐齐哈尔市梅里斯达斡尔族区'
id.old[2536,2] <- '云南省镇沅彝族哈尼族拉祜族自治县'
id.old[2583,2] <- '云南省双江拉祜族佤族布朗族傣族自治县'
id.old[2858,2] <- '甘肃省积石山保安族东乡族撒拉族自治县'

id.old$province <- ''
id.old$city1 <- ''
for (i in 1:nrow(id.old)){
  if (substring(id.old[i,2],1,2) %in% c('新疆','西藏','宁夏','广西')){
    id.old[i,3] <- substring(id.old[i,2],1,2)
    id.old[i,4] <- substring(id.old[i,2],3,nchar(id.old[i,2]))
  }else{
    id.old[i,3] <- substring(id.old[i,2],1,3)
    id.old[i,3] <- gsub("省","",id.old[i,3])
    id.old[i,3] <- gsub("市","",id.old[i,3])
    id.old[i,4] <- substring(id.old[i,2],4,nchar(id.old[i,2]))
  }
}
id.old <- id.old[,.(scan_id,province,city1)]
colnames(id.old)[3] <- 'city'
id.new <- id.new[,.(scan_id,province,city)]
id.new <- unique(id.new);id.old <- unique(id.old)
# 处理id.new中很多的同编码对应多个城市
id.new.n <- id.new[,.(n=length(city)),.(scan_id)]
id.new.n <- subset(id.new.n,n>1)
id.new.coincide <- merge(id.new.n,id.new,by.x = 'scan_id',by.y = 'scan_id')
id.new.coincide <- id.new.coincide[,.(city=toString(city)),.(scan_id,province)]
id.new.coincide[10,3] <- '澳门'
id.new.coincide[9,3] <- '香港'
id.new.n <- id.new[,.(n=length(city)),.(scan_id)]
id.new.n <- subset(id.new.n,n==1)
id.new.noncoincide <- merge(id.new.n,id.new,by.x = 'scan_id',by.y = 'scan_id');id.new.noncoincide <- id.new.noncoincide[,.(scan_id,province,city)]
id.new <- funion(id.new.coincide,id.new.noncoincide)

# 处理新旧两代编码的重合部分,交集
id.join <- merge(id.old,id.new,by.x='scan_id',by.y = 'scan_id')
id.join <- id.join[,.(scan_id,province.x,city.x)]
colnames(id.join) <- c('scan_id','province','city')
id.join <- unique(id.join)

# 找出新旧两代编码不重合部分
id.new <- subset(id.new,!scan_id %in% id.join$scan_id)
id.old <- subset(id.old,!scan_id %in% id.join$scan_id)
# 合并
id.all <- funion(id.new,id.old) %>%
  funion(id.join)
id.all <- unique(id.all)
write.csv(id.all,"身份证户籍.csv",fileEncoding='UTF-8',row.names=FALSE)
