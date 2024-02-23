from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import sys

appName = 'PySpark_Initialise'

spark = SparkSession.builder.appName('Session_Initialize').getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {spark.sparkContext.appName}')
    print(f'Master: {spark.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

#==============================================================================================================

hostname = "database-1.cxgwrra2uy2f.ap-south-1.rds.amazonaws.com"
jdbcPort = 3306
dbname = 'project_db'
username = 'admin'
password = '11112222'
#table = "demo_table"

jdbc_url = "jdbc:mysql://{0}:{1}/{2}".format(hostname, jdbcPort, dbname)

connectionProperties = {
  "user" : "admin",
  "password" : "11112222"
}

print("---------connection establishes till this point--------")

#=============================================================================================================

#creating partition name from input provided by user


user_input=str(sys.argv[1])
user_input_string=user_input.replace("-","")
partition_input="date="+user_input_string

#that day's file will be rrad in a dataframe df
df=spark.read.parquet("hdfs://localhost:9000/final_project/temp/output/%s" %partition_input)

#droppping neutral news
df2=df.where(df.sentiment != '[neutral]')

#============================================================================================================

#create a temp view on that loaded dataframe the view name is : news
df2.createOrReplaceTempView("news")



#---> part0: querying data: save all data on mysql server

q=spark.sql("SELECT * from news")
q.show(10)
q.write.jdbc(jdbc_url, 'all_news', 'overwrite', properties={"user": username, "password": password})



#---> part1: querying data: overall sentiment for news for one day irrespective of domain (all financial news)

# finding out total count of each sentiment irrespective of domain
# this will be one csv file which can be plotted for negative and positive for overall sentiment
print("sentiment for all news: ")
q1 = spark.sql("SELECT sentiment, count(tags) as temp_count FROM news GROUP BY sentiment")
q1=q1.withColumn("tags", lit("all_news"))
q1.show(10)

q1.write.jdbc(jdbc_url, 'overall_sentiment', 'overwrite', properties={"user": username, "password": password})



#---> part2: querying data: sentiment for news for "one day" for "each domain"

#finding out total count of each sentiment of a specific domain
print("sentiment for specific domain: ")

# 1. auto
auto= spark.sql("SELECT sentiment, count(tags) as temp_count FROM news WHERE tags REGEXP 'auto' GROUP BY sentiment")
auto=auto.withColumn("tags", lit("auto"))
auto.show()

# 2. banking
banking= spark.sql("SELECT sentiment, count(tags) as temp_count FROM news WHERE tags REGEXP 'banking' GROUP BY sentiment")
banking=banking.withColumn("tags", lit("banking"))
banking.show()

# 3. tech
tech= spark.sql("SELECT sentiment, count(tags) as temp_count FROM news WHERE tags REGEXP 'tech' GROUP BY sentiment")
tech=banking.withColumn("tags", lit("tech"))
tech.show()

# 4. pharma
pharma= spark.sql("SELECT sentiment, count(tags) as temp_count FROM news WHERE tags REGEXP 'pharma' GROUP BY sentiment")
pharma=banking.withColumn("tags", lit("pharma"))
pharma.show()

# 5. energy
energy= spark.sql("SELECT sentiment, count(tags) as temp_count FROM news WHERE tags REGEXP 'energy' GROUP BY sentiment")
energy=banking.withColumn("tags", lit("energy"))
energy.show()

#joining all domains queries outputs

auto_bank=auto.unionAll(banking)
auto_bank_tech=auto_bank.unionAll(tech)
auto_bank_tech_pharma=auto_bank_tech.unionAll(pharma)
auto_bank_tech_pharma_energy=auto_bank_tech_pharma.unionAll(energy)
auto_bank_tech_pharma_energy.show()


auto_bank_tech_pharma_energy.write.jdbc(jdbc_url, 'auto_bank_tech_pharma_energy', 'overwrite', properties={"user": username, "password": password})



#---> part3: querying data: top 10 news of one specific day irrespective of domain (based on epoch) all news
print("top 10 news from all domains: ")
top_10 = spark.sql("SELECT distinct(title), description, epoch, sentiment, tags from news ORDER BY epoch DESC limit 10")
top_10.show(truncate=True)

top_10.write.jdbc(jdbc_url, 'top_10', 'overwrite', properties={"user": username, "password": password})


#---> part4: querying data: top 10 news of one specific day of a specific domain (based on epoch)

print("top 10 news: domain specific: ")
# Top 10 latest news by domain
top_10_auto = spark.sql("SELECT title, epoch,  sentiment, tags FROM news WHERE tags REGEXP 'auto' ORDER BY epoch DESC limit 10")
top_10_auto.show(truncate=False)

top_10_bank = spark.sql("SELECT title, epoch,  sentiment, tags FROM news WHERE tags REGEXP 'banking' ORDER BY epoch DESC limit 10")
top_10_bank.show(truncate=False)

top_10_tech = spark.sql("SELECT title, epoch,  sentiment, tags FROM news WHERE tags REGEXP 'tech' ORDER BY epoch DESC limit 10")
top_10_tech.show(truncate=False)

top_10_pharma = spark.sql("SELECT title, epoch,  sentiment, tags FROM news WHERE tags REGEXP 'pharma' ORDER BY epoch DESC limit 10")
top_10_pharma.show(truncate=False)

top_10_energy = spark.sql("SELECT title, epoch,  sentiment, tags FROM news WHERE tags REGEXP 'energy' ORDER BY epoch DESC limit 10")
top_10_energy.show(truncate=False)

a=top_10_auto.unionAll(top_10_bank)
b=a.unionAll(top_10_tech)
c=b.unionAll(top_10_pharma)
top_10_domains=c.unionAll(top_10_energy)

top_10_domains.show(50, truncate=False)


top_10_domains.write.jdbc(jdbc_url, 'top_10_domains', 'overwrite', properties={"user": username, "password": password})

print("data successfully written to mysql database!!!")
