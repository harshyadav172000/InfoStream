import warnings
warnings.simplefilter("ignore")
import pyspark
import re
import requests
import pandas as pd
import dateutil.parser
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
import sys



#creating a spark session
spark=SparkSession.builder.appName("new fetch app").master("local").getOrCreate()



#user input as date in the yyyy-mm-dd format
user_input_string=str(sys.argv[1])  #input in string format
user_input_date= dateutil.parser.parse(user_input_string)     # convert string to datetime format yyyy-mm-dd hh:mm:ss
user_input_only_date = user_input_date.date()                 # extract only date from date time for comparison later

folder_name = user_input_string.replace("-","")               # to create folders structure



#create empty pandas dataframe as a temporary data holder
df = pd.DataFrame()
pd.set_option('display.max_columns', None)



# url list file
url_tags_file = open('/home/sidd0613/final_project/url_tags_updated.txt', 'r', newline='')



#news fetch logic main code
print("fetching",end="")
while True:

    print(".",end="")

    content = url_tags_file.readline()

    if not content:
        break

    link, tags = content.split()
    
    url = requests.get(link)

    soup = BeautifulSoup(url.content, 'xml')

    items = soup.find_all('item')


    for item in items:
        list = []

        tag = tags

        title = item.title.text


        if item.description:
            description = item.description.text
            x = re.compile("<a.*a>")
            description = re.sub(x, '', description).strip()
   		
        else:
            description = "No description"
      

        pubDate = item.pubDate.text

        try:
            publish_date = dateutil.parser.parse(pubDate)

        except dateutil.parser._parser.ParserError:
            continue

        # extracting date from yourdate 
        only_date = publish_date.date()

        # converting to timestamp
        epoch = publish_date.timestamp()



        if (only_date == user_input_only_date):

            only_date=str(only_date).replace("-","")
            list.append([only_date, epoch, title, description, tag])


            df = df.append(
                pd.DataFrame(list, columns=['date', 'epoch', 'title', 'description', 'tags']),
                ignore_index=True)



#create spark dataframe from pandas dataframe
sparkDF = spark.createDataFrame(df)
sparkDF.printSchema()
sparkDF.show(30,truncate=True)

#writing spark dataframe on hdfs in delta format
sparkDF.write.format("parquet").mode("overwrite") \
    .save("hdfs://localhost:9000/final_project/temp/%s/" % folder_name)

print("data written successfully on hdfs!!!")



