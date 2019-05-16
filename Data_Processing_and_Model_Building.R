
setwd("C:/COE Contents/SparkR/New Content/")

options(scipen = 999)

library(rJava)

Sys.setenv(SPARK_HOME="C://spark-2.2.0-bin-hadoop2.7")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"),"R","lib")))

sparkR.session(enableHiveSupport=FALSE,master="local[*]",
               sparkConfig=list(spark.driver.memory="2g"))


# Loading Data
housing = read.df("C:/Users/rshars/Desktop/Shared drive/Spark R/Week 2/housing.csv",
                  source = "csv",
                  header = TRUE,
                  inferschema = TRUE)

# Creating summary of data
dd = summary(housing)
tt = collect(dd)

# checking dimensions
nrow(housing)
ncol(housing)
dim(housing)

# Creating R data frame using collect function (should not be used for large dataset)
housing_r_dataframe = collect(housing)

# Creating R data frame using as.data.frame function 
housing_r_dataframe1 = as.data.frame(housing)


#------------------------------------------------------------------------------------------------
#------------------------ Outlier Treatment -----------------------------------------------------
#------------------------------------------------------------------------------------------------
outlierDetection = function(x){
  for(i in colnames(x)){
    if(!(class(x[,i]) %in% c('character','factor'))){
      Q1 = quantile(x = x[,i],probs = 0.25, na.rm = T)
      Q3 = quantile(x = x[,i],probs = 0.75, na.rm = T)
      IQR = Q3 - Q1
      x[,i] = ifelse(is.na(x[,i]),NA,ifelse(x[,i]<Q1-IQR,NA,x[i]))
      x[,i] = ifelse(is.na(x[,i]),NA,ifelse(x[,i]>Q3+IQR,NA,x[i]))      
    }
  }
  return(x)
}

housing_outlier = dapply(x = housing, func = outlierDetection, schema = schema(housing))

# collecting output in r data frame (not suggested for large data size)
gg = collect(housing_outlier)


#------------------------------------------------------------------------------------------------
#----------------------- Missing Value Treatment ------------------------------------------------
#------------------------------------------------------------------------------------------------

# 1. Removing rows where missing valies are there (for all the columns)
mrlist = colnames(housing_outlier)
housing1 <- dropna(housing_outlier, cols = mrlist)
nrow(housing1)

# removing rows where all values are missing (2 rows are completely missing)
housing_all <- dropna(housing_outlier, how = "all")
nrow(housing_all)    

# removing rows where any one column is missing (11 rows have missing values)
housing_any <- dropna(housing_outlier, how = "any")
(n_any <- nrow(housing_any))

# we can also remove rows if it has at least mentioned number of non-missing values
housing_more_than_2 <- dropna(housing_outlier, minNonNulls = 2)
nrow(housing_more_than_2)

# 2. Imputing missing values by constant (fillna works only for numeric columns)
housing_filled <- fillna(housing_outlier, value = 0)  # imputing all column by same value (0)
str(housing_filled)
nrow(housing_filled)

# character variable not imputed
count(where(housing_filled, isNull(housing_filled$Var)))
# character can be replaced using if ifelse statement
housing_filled$Var <- ifelse(isNull(housing_filled$Var), "Unknown", housing_filled$Var)
count(where(housing_filled, isNull(housing_filled$Var)))  # no missing values left
str(housing_filled) 
showDF(housing_filled, 6)


# 3. Imputing by mean for numeric and mode for character
# registering table for SQL queries 
createOrReplaceTempView(housing_outlier, 'housing_outlier')
# list should only have numeric columns or values should be replaced by mode 
# for character values
L = list()
# finding mean of numeric column
for(i in colnames(housing_outlier)[coltypes(housing_outlier)!='character']){
  L[[i]] = collect(sql(paste('select avg(',i,') as ',i,'from housing_outlier')))[1,1]
}
# finding mode of character columns
for(i in colnames(housing_outlier)[coltypes(housing_outlier)=='character']){
  L[[i]] = collect(sql(paste('select ',i,' as ',i,
                             'from housing_outlier group by ',i,
                             ' order by count(*) DESC')))[1,1]
}
housing_filled1 = fillna(x = housing_outlier, value = L)


# 4. Imputing missing values by median
# registering table for SQL queries 
createOrReplaceTempView(housing_outlier, 'housing_outlier')
# list should only have numeric columns or values should be replaced by mode 
# for character values
L = list()
# finding median of numeric column
for(i in colnames(housing_outlier)[coltypes(housing_outlier)!='character']){
  L[[i]] = collect(sql(paste('select percentile_approx(',i,',0.5) as ',i,' from housing_outlier')))[1,1]
}
# fincding mode of character columns
for(i in colnames(housing_outlier)[coltypes(housing_outlier)=='character']){
  L[[i]] = collect(sql(paste('select ',i,' as ',i,
                             'from housing_outlier group by ',i,
                             ' order by count(*) DESC')))[1,1]
}
housing_filled2 = fillna(x = housing_outlier, value = L)

#----------------------------------------------------------------------------------------------
#----------------------- Variable Transformation ----------------------------------------------
#----------------------------------------------------------------------------------------------

# Creating new derived column
housing_filled2 <- withColumn(housing_filled2, "new_column1", housing_filled2$PTRATIO^2)
housing_filled2 <- withColumn(housing_filled2, "new_column2", log(housing_filled2$PTRATIO))
housing_filled2 <- withColumn(housing_filled2, "new_column3", exp(housing_filled2$PTRATIO^0.5))

# dropping columns
X = housing_filled2
X$MEDV = NULL  # drop(X, 'MEDV') can also achive the same

# PCA not available for R API of Spark as of now


#----------------------------------------------------------------------------------------------
#----------------------- Splitting Data into Train and Test -----------------------------------
#----------------------------------------------------------------------------------------------

# Sample data randomly
dim(housing_filled2)
sample_1 = sample(x = housing_filled2, withReplacement = FALSE, fraction = 0.7, seed = 123)
dim(sample_1)

# Stratified random sample
housing_filled2$CHAS = cast(housing_filled2$CHAS, 'string')
sample_2 = sampleBy(x = housing_filled2, col = "CHAS", fractions = list('0'=0.7,'1'=0.7), seed = 123)
dim(sample_2)

# Data splitting for model building
trainTest <-randomSplit(housing_filled2,c(0.7,0.3), seed=123)
train = trainTest[[1]]
test = trainTest[[2]]
dim(train)
dim(test)


#----------------------------------------------------------------------------------------------
#----------------------- Model Fitting --------------------------------------------------------
#----------------------------------------------------------------------------------------------

#--------------------------- Fitting glm model -----------------------------------------------

glm_model = spark.glm(data = train, formula = MEDV~., family = "gaussian")

results = summary(glm_model)
coef = results$coefficients
AIC = results$aic

# scoring (adds prediction as last column in newData)
X_test = test
y_test = test$MEDV
X_test$MEDV = NULL
df = predict(glm_model, newData = test)

# Calculating various performance metrices
createOrReplaceTempView(df,'df')
df$y_avg = collect(sql("select avg(label) as label from df"))[1,1]
df$y_hat = df$prediction
df$sq_res = (df$label - df$prediction)^2
df$sq_tot = (df$label - df$y_avg)^2
df$res = (df$label - df$prediction)

createOrReplaceTempView(df,'df')
SSR = collect(sql('select avg(sq_res) from df'))[1,1]
MAE = collect(sql('select avg(abs(res)) from df'))[1,1]
RSquare = 1 - collect(sql("select sum(sq_res)/sum(sq_tot) from df"))[1,1]

# writing and loading model object in folder
write.ml(glm_model, path = "C:/COE Contents/SparkR/New Content/model_result/glm_model")
savedModel <- read.ml(path="C:/COE Contents/SparkR/New Content/model_result/glm_model")



#----------------------------------------------------------------------------------------------------
#--------------------- Other Regression Models ------------------------------------------------------
#----------------------------------------------------------------------------------------------------
#---------- Fit a random forest regression model with spark.randomForest ----------------------------
#----------------------------------------------------------------------------------------------------

model_rf_reg <- spark.randomForest(train, MEDV ~ ., "regression", numTrees = 30)
rf_reg_result = summary(model_rf_reg)

# finding variable importance
a = rf_reg_result$featureImportances
a = strsplit(gsub('\\]','',gsub('\\[','',gsub("\\(","",gsub('\\)','',a)))),',')[[1]]
features = c()
importance = c()
for(i in 1:length(rf_reg_result$features)){
  features = c(features, rf_reg_result$features[[i]])
  importance = c(importance, a[length(rf_reg_result$features)+1+i])
}
var_importance = data.frame(Variable = features, 
                            Importance = importance)
var_importance$Importance = as.numeric(as.character(var_importance$Importance))

# resuls can be analysed in the same manner as was for linear regression (glm)
rf_reg_scored = predict(model_rf_reg, newData = test) 

#----------------------------------------------------------------------------------------------------
#---------- Fit a random forest classification model with spark.randomForest ------------------------
#----------------------------------------------------------------------------------------------------

train1 = train
train1$Y = ifelse(train1$MEDV>20,'Yes','No')
train1$MEDV = NULL
test1 = test
test1$Y = ifelse(test1$MEDV>20,"Yes","No")
model_rf_cls <- spark.randomForest(train1, Y ~ ., "classification", numTrees = 30)

rf_reg_result = summary(model_rf_cls)

# finding variable importance
a = rf_reg_result$featureImportances
a = strsplit(gsub('\\]','',gsub('\\[','',gsub("\\(","",gsub('\\)','',a)))),',')[[1]]
features = c()
importance = c()
for(i in 1:length(rf_reg_result$features)){
  features = c(features, rf_reg_result$features[[i]])
  importance = c(importance, a[length(rf_reg_result$features)+1+i])
}
var_importance = data.frame(Variable = features, 
                            Importance = importance)
var_importance$Importance = as.numeric(as.character(var_importance$Importance))

rf_cls_scored = predict(model_rf_cls, newData = test1)

rf_cls_scored$TP = ifelse((rf_cls_scored$Y==rf_cls_scored$prediction)&(rf_cls_scored$Y=="Yes"),1,0)
rf_cls_scored$TN = ifelse((rf_cls_scored$Y==rf_cls_scored$prediction)&(rf_cls_scored$Y=="No"),1,0)
rf_cls_scored$FP = ifelse((rf_cls_scored$Y=="No")&(rf_cls_scored$prediction=="Yes"),1,0)
rf_cls_scored$FN = ifelse((rf_cls_scored$Y=="Yes")&(rf_cls_scored$prediction=="No"),1,0)

createOrReplaceTempView(rf_cls_scored, "rf_cls_scored")

perf_table = collect(sql("select sum(TP) as TP, 
                         sum(TN) as TN, 
                         sum(FP) as FP, 
                         sum(FN) as FN from rf_cls_scored"))
perf_table$Positive = perf_table$TP + perf_table$FN
perf_table$Negative = perf_table$TN + perf_table$FP
perf_table$Total = perf_table$Positive + perf_table$Negative
perf_table$Precision = perf_table$TP/(perf_table$TP+perf_table$FP)
perf_table$Recall = perf_table$TP/(perf_table$Positive)
perf_table$Accuracy = (perf_table$TP+perf_table$TN)/perf_table$Total

#----------------------------------------------------------------------------------------------
#-------------------- Plotting in SparkR ------------------------------------------------------
#----------------------------------------------------------------------------------------------
a = histogram(housing_filled2, 'MEDV', nbins = 5)

