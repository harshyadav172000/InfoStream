from pyspark.sql import SparkSession
from sparknlp import Finisher
from pyspark.ml import Pipeline
from sparknlp.pretrained import PretrainedPipeline
import datetime
import sys



spark = SparkSession.builder \
    .appName("Spark NLP")\
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.executor.memory","1G")\
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.2").getOrCreate()



user_date_input_string=str(sys.argv[1]) #user input in string format
user_input_as_folder_name=user_date_input_string.replace("-","")



#read deta file into spark df
df = spark.read.format("parquet").load('hdfs://localhost:9000/final_project/temp/%s/' %user_input_as_folder_name)
df.show(20, truncate=True)



text_df=df.select("description").withColumnRenamed("description", "text")
text_df.show(20, truncate=True)



#======================================================================================================================================

#nlp pipeline 
finisher = Finisher().setInputCols(["class"])

#loading from local
analyze = PretrainedPipeline.from_disk("file:///home/PR1/final_project/classifierdl_bertwiki_finance_sentiment_pipeline_en_3.3.0_2.4_1636617651675").model

pipeline = Pipeline().setStages([analyze,finisher])

model = pipeline.fit(text_df)

annotations_df = model.transform(text_df)

annotations_df_2 = annotations_df.withColumn("sentiment",annotations_df["finished_class"].cast('string'))
annotations_df_2=annotations_df_2.drop("finished_class")


print("final result is this: ")
annotations_df_2.show(20,truncate=True)


#======================================================================================================================================




#join dataframe with sentiment column
final_df=df.join(annotations_df_2, df.description == annotations_df_2.text, "inner").distinct().drop("text")
print("this is final data: ")
final_df.show(20,truncate=True)


#writing to hdfs
final_df.write.partitionBy("date").mode("overwrite").parquet("hdfs://localhost:9000/final_project/temp/output/")


print("successfully written to hdfs!!!")

#======================================================================================================================================
