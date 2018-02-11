# 转编码为UTF-8的data.frame 成 编码为CP936
# 用于RMySQL取出的含中文data.frame

utf8tocp936 <- function(df){
  colnames(df) <- iconv(colnames(df),'UTF-8','CP936')
  for (i in 1:ncol(df)){
    df[,i] <- iconv(df[,i],'UTF-8','CP936')
  };
  df
}
