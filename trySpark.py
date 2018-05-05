from pyspark import SparkContext, SparkConf
from petrarch2 import petrarch2



conf = SparkConf().setAppName("EventCoding").setMaster("local[4]")
sc = SparkContext(conf=conf)

mongoDF = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
"mongodb://127.0.0.1/people.contacts").load()

mongoRdd = mongoDF.rdd.map(list)

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

finalRdd = mongoRDD.map(eventCoding)