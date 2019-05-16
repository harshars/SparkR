

options(scipen = 999)

library(rJava)

# if(nchar(Sys.getenv("SPARK_HOME"))<1){
#   Sys.setenv(SPARK_HOME="C://spark-2.2.0-bin-hadoop2.7")
# }

Sys.setenv(SPARK_HOME="C://spark-2.2.0-bin-hadoop2.7")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"),"R","lib")))

sparkR.session(enableHiveSupport=FALSE,master="local[*]",
               sparkConfig=list(spark.driver.memory="2g"))


data("iris")
colnames(iris) = c("SepalLength","SepalWidth","PetalLength","PetalWidth","Species")
a = iris
a$Species = NULL

# creating spark data frame from R data.frame
idf = createDataFrame(iris)
adf = createDataFrame(a)

# user defined functions on spark data
col_sum = function(x){
  x = rbind(x, apply(x, MARGIN = 2, FUN = sum))
  return(x)
}

# function passed should return data.frame (adds column sum as last row)
d1 = dapply(adf, func = col_sum, schema = schema(adf))
View(collect(d1))

# some more functions 
col_sum1 = function(x){
  y = as.data.frame(apply(x, MARGIN = 2, FUN = sum))
  return(y)
}

# function passed should return data.frame
d2 = dapplyCollect(adf, func = col_sum1)
View(d2)

# some more functions 
col_sum2 = function(x){
  y = as.data.frame(data.frame(col_sums = apply(x, MARGIN = 2, FUN = sum)))
  return(y)
}

# function passed should return data.frame
d3 = dapplyCollect(adf, func = col_sum2)
View(d3)

# some more functions - row sum 
col_sum3 = function(x){
  y = (apply(x, MARGIN = 1, FUN = sum))
  return(y)
}
# function passed should return data.frame
d4 = dapplyCollect(adf, func = col_sum3)
View(d4)

# some more operations using dapply



# Building ML modoels in sparkR
# Regression
housing = read.df("C:/Users/krishna/Documents/kmroy_R_code/sparkR COE/housing.csv",
                  source = "csv",
                  header = TRUE,
                  inferschema = TRUE)

housing1 = read.csv("C:/Users/krishna/Documents/kmroy_R_code/sparkR COE/housing.csv")
# model1 = glm(data = housing1, MEDV~., family = "gaussian")
# summary(model1)

model = spark.glm(data = housing, MEDV~., family = "gaussian")
summary(model)

output = predict(object = model, housing)

MAPE = function(x){
  actual = "MEDV"
  predicted = "prediction"
  
  y = mean(abs((x[,actual]-x[,predicted])/x[,actual]))
  return(y)
  
}
# calculating MAPE
dapplyCollect(output, func = MAPE)
# getting model parameters
a = summary(model)






