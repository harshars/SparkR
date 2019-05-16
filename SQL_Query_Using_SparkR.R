
options(scipen = 999)
library(rJava)


Sys.setenv(SPARK_HOME="C://spark-2.2.0-bin-hadoop2.7")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"),"R","lib")))
sparkR.session(enableHiveSupport=FALSE,master="local[*]",
               sparkConfig=list(spark.driver.memory="2g"))


# using sql query 
housing = read.df("C:/COE Contents/SparkR/New Content/housing.csv",
                  source = "csv",
                  header = TRUE,
                  inferschema = TRUE)

# registering data frame as sql table
createOrReplaceTempView(housing, "housing_data")

# filtering data based on condition/s
dd = sql("select * from housing_data where ZN>5")
head(dd)
# some aggregation operations using sql query
dd = sql("select distinct CHAS, 
          sum(CRIM) as CRIM_SUM,
          avg(ZN) as ZN_AVG from housing_data where INDUS<=5 group by CHAS")
head(dd)
# aggregation and merging data using sql query
dd = sql("select distinct a.CHAS,
          a.CRIM_SUM, b.ZN_AVG from 
          (select distinct CHAS, sum(CRIM) as CRIM_SUM from housing_data group by CHAS) as a left join 
          (select distinct CHAS, avg(ZN) as ZN_AVG from housing_data group by CHAS) as b 
          on a.CHAS=b.CHAS")
head(dd)

