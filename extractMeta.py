from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql import SparkSession
from pycorenlp import StanfordCoreNLP

import xml.etree.ElementTree as ET
import json


nlp = StanfordCoreNLP('http://localhost:9000')

conf = SparkConf().setAppName("newsExtraction").setMaster("local[4]")
conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.2")

sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
    .getOrCreate()
    
newsRdd = sc.textFile("out.xml")
def extractMeta(line):
    root = ET.fromstring(line)
    record = {}
    record.update(root.attrib)
    oriText = ""
    output = None
    for child in root:
        if(child.tag != "TEXT"):
            record.update({child.tag: child.text})
    for p in root.iter('P'):
        oriText += (p.text + " ")
    output = nlp.annotate(oriText, properties={
    # 'annotators': 'tokenize,ssplit,sentiment,depparse,parse',
    'annotators': 'tokenize, parse, ssplit',
    'outputFormat': 'json'
    })
    for sentenceDict in output['sentences']:
        sentence  = ""
        for token in sentenceDict['tokens']:
            sentence += (token['originalText'] +" ")
        currentIndex = sentenceDict['index']
        sentenceDict['text'] = sentence
        sentenceDict['index'] = str(sentenceDict['index'])
        targetKeys = ['index', 'text', 'parse']
        result = dict((key, sentenceDict[key]) for key in targetKeys)
        output['sentences'][currentIndex] = result
    record.update(output)
    return record
record = newsRdd.map(extractMeta)

recordDF = record.toDF()

recordDF.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database",
"political_event").option("collection", "records").save()
recordDF.show()
