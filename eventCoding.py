from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql import SparkSession
from petrarch2 import petrarch2

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

df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
"mongodb://127.0.0.1/political_event.records").load()

mongoRDD = df.rdd

def eventCoding(doc):
    # doc[2] = "CNA_ENG_19971001.0001"
    docMeta = doc['id'].split("_")
    source = docMeta[0]
    doc_id = docMeta[0] + docMeta[2]
    date = docMeta[2].split(".")[0]

    # meta is the metadata for event coding
    # meta = {
    #     "date": "11112233",
    #     "id": "story_sent#",
    #     "source": "AFP",
    #     "sentence": "True",
    #     "Text": "adssdafsad",
    #     "Parse": "lkjsadfwef"
    # }

    # doc[3] is the sentences dictionary
    for sentence in doc['sentences']:
        meta = {}
        id = doc_id + "_" + sentence['index']
        meta['id'] = id
        meta['date'] = date
        meta['source'] = source
        meta['sentence'] = "True"
        meta['Text'] = sentence['text']
        meta['Parse'] = sentence['parse']
        petrarch2.main(meta)

    return 0

finalRdd = mongoRDD.map(eventCoding).collect()
print mongoRDD.collect()
