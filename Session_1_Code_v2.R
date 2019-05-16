#Steps for creating sparkR environment:
#Step1:
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "C://Apache/Spark/spark-2.0.0-bin-hadoop2.7/spark-2.0.0-bin-hadoop2.7/")
  }
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[*]",appName = "SparkR", sparkEnvir = list(spark.driver.memory="4g"))
sqlContext <- sparkRSQL.init(sc) 
#Step2:
sparkR.session(enableHiveSupport = FALSE,master = "local[*]", sparkConfig = list(spark.driver.memory = "4g",spark.sql.warehouse.dir="C://Apache/winutils"))
nchar(Sys.getenv('SPARK_HOME'))

library(sparklyr)
#Creating sparkR dataset from existing dataset:
df = as.DataFrame(faithful)
df2 = createDataFrame(faithful)

#Loading CSV files in sparkR
auction_data = read.df('D:/Personal Study/COE/SPARK R/auction.csv','csv',inferSchema = 'True', header = 'True')
class(auction_data)
#To see the tructure of the data
str(auction_data)

#To see first few rows of data
head(auction_data,2) # default number of rows it will show is 6
showDF(df2,5) #by default it shows 20 rows

#Column Names
colnames(auction_data)

#To check the first few values of a column
# In sparkR, commands like auction_data$bid[5] will not work as s4 object type is not subsettable.
#so, if you want to see first few values of particular column, you can do it in the below mentioned way:

# If you want to see the 5th value of bid column, you can do this by following: 
all_5 = head(auction_data,5)$bid
fifth_value = tail(all_5,1)

#For getting a particular row
all50 <- take(auction_data,50)
row50 <- tail(all50,1)

#To convert a subset of sparkdataframe into R dataframe for granular analysis
localDF = collect(auction_data)
class(localDF)

# Changing Datatype in sparkR
#Changing datatype into string
auction_data$auctionid = cast(auction_data$auctionid,'string')
str(auction_data)

#Changing datatype into numeric
auction_data$auctionid = cast(auction_data$auctionid,'integer')
str(auction_data)

#Changing datatype into Date (if column is formatted according to R date format)
df <- createDataFrame(data.frame(purchase_date=c('2015-02-04', '2014-03-10')))
View(df)
str(df)
df$purchase_data = cast(df$purchase_date,"date")

#Changing datatype into Date (if column is not formatted according to R date format)
df <- createDataFrame(data.frame(purchase_date=c('04/02/2015', '03/10/2014')))
dt <- cast(cast(unix_timestamp(df$purchase_date, 'MM/dd/yyyy'),'timestamp'),'date')
df$format_date = dt
str(df)

# Calculating difference between two dates
Date_data = read.df('D:/Personal Study/COE/SPARK R/Date_Data.csv','csv',inferSchema = 'True', header = 'True')
str(Date_data)
dt <- cast(cast(unix_timestamp(Date_data$Date1, 'dd-MM-yyyy'),'timestamp'),'date')
Date_data$Date1 = dt
dt_2 <- cast(cast(unix_timestamp(Date_data$Date2, 'dd-MM-yyyy'),'timestamp'),'date')
Date_data$Date2 = dt_2
Date_data$date_two_difference= datediff(Date_data$Date2,Date_data$Date1)
View(Date_data)

#Subsetting data using multiple condition
subset_auction_data = auction_data[auction_data$bid>=500 & auction_data$auctionid==1643075711,]
View(subset_auction_data)

#Subsetting using subset function
auction_data_2 =subset(auction_data, auction_data$auctionid==1643075711 & auction_data$bid>=500)
View(auction_data_2)

#Subsetting data using %in% statement
auction_data_in_subset =subset(auction_data,auction_data$bid %in% c(400:500))
View(auction_data_in_subset)
#ss = min(auction_data_in_subset$bid)

#Subsetting using filter function
auction_data_3 = filter(auction_data,auction_data$auctionid==1643075711)
View(auction_data_3)

#Binding two different dataset using rbind
auction_data_bind = rbind(auction_data_2,auction_data_3)

#auction_data_3$item = gsub(' ','',auction_data_3$item)

# Creating New variables
aa= auction_data
aa$auctionid_2 = log(aa$auctionid)
View(aa)

# Creating using ifelse statement
aa$new_flag = ifelse(aa$auctionid==1643075711,1,0)
View(aa)

# Using substr for creating new variable
aa$bid_trim = substr(aa$bid,1,3)
View(aa)

#Creating new variable using withcolumn statement
auction_data_dup = withColumn(auction_data, 'col2',auction_data$bid*2)
View(auction_data_dup)

#Splitting columns into multiple 
library(magrittr)

df <- createDataFrame(data.frame(
  categories=c("cat1,cat2,cat3", "cat1,cat2", "cat3,cat4", "cat5")
))
View(df)

separated <- selectExpr(df, "split(categories, ',') AS categories")
View(separated)

#Using levenshtein(y, x) distance
aa$distance = levenshtein(aa$bidder, aa$item)
View(aa)

#Calculate mean of a column
mean_open_bid = agg(auction_data, mean = mean(auction_data$openbid))
typeof(mean_open_bid)

#Calculate variance of a column
variance_open_bid = agg(auction_data, variance = var(auction_data$openbid))
typeof(variance_open_bid)

#Calculate standard deviance of a column
std_open_bid = agg(auction_data,  stand_dev= stddev(auction_data$openbid))
typeof(std_open_bid)
View(std_open_bid)

#Calculate max,min and range of a column
max_min_range <- agg(auction_data, minimum = min(auction_data$openbid), maximum = max(auction_data$openbid), 
                range_width = abs(max(auction_data$openbid) - min(auction_data$openbid)))

#Summarize based on particular variable
bidder= summarize(groupBy(auction_data, auction_data$bidder), sum = sum (auction_data$bid))
head(arrange(bidder, desc(bidder$sum)))

bidder_counts <- summarize(groupBy(auction_data, auction_data$bidder), count = n(auction_data$bidder))
head(arrange(bidder_counts, desc(bidder_counts$count)))

#Summarize based on multiple variables
head(agg(rollup(auction_data, "bidder", "item"), avg(auction_data$price)))

#Calculate null values in each column in each dataset
missing_count <- dapplyCollect(
  auction_data,
  function(auction_data) {
    apply(auction_data, 2, function(col) {sum(is.na(col))})
  })

View(auction_data)

#Identify the missing value and  Fillna to impute missing values
filter(auction_data, isNull(auction_data$bidder)) %>% head()
auction_data_2 = fillna(auction_data, "charltonfranklin", cols = "bidder")
filter(auction_data_2, isNull(auction_data_2$bidder)) %>% head()

#Using SQL to create new column calculating mean
createOrReplaceTempView(auction_data,"auction")

results <- sql(
  "SELECT auctionid, item,  avg(bid) FROM auction
    GROUP BY auctionid, item"
)
class(results)
View(results)

# Loop in sparkR
for (i in ncol(df2)){
  print(df2[,i])
}


################################################
#Substitute a particular part of variable
#auction_data_3$item = gsub(' ','',auction_data_3$item)

#how many bids per item
groupbyitem=groupBy(auction_data,auction_data$item)
countbyitem=count(groupbyitem)
aa = collect(countbyitem)
class(aa)
class(groupbyitem)

#Using SQL to create new column calculating mean
createOrReplaceTempView(auction_data,"auction")

results <- sql(
  "SELECT auctionid, item,  count(bid) FROM auction
    GROUP BY auctionid, item"
)
class(results)

