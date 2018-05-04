from pyspark import SparkContext, SparkConf
from pycorenlp import StanfordCoreNLP
import xml.etree.ElementTree as ET


nlp = StanfordCoreNLP('http://localhost:9000')

conf = SparkConf().setAppName("newsExtraction").setMaster("local[4]")
sc = SparkContext(conf=conf)
newsRdd = sc.textFile("out.xml")
def extractMeta(line):
    root = ET.fromstring(line)
    record = {}
    record.update(root.attrib)
    output = None
    for child in root:
        if(child.tag != "TEXT"):
            record.update({child.tag: child.text})
    for p in root.iter('P'):
        output = nlp.annotate(p.text, properties={
  'annotators': 'tokenize,ssplit,sentiment,depparse,parse',
  'outputFormat': 'json'
  })
    record.update(output)
    return record
record = newsRdd.map(extractMeta).collect()
print record
