from lxml import etree

parser = etree.XMLParser(remove_blank_text=True)


with open("cna_eng_199709.xml", "r") as inputFile:
  fileContent = inputFile.read()
  # root = ET.fromstring("<fake>" + fileContent +"</fake>")
  root = etree.XML(fileContent, parser=parser)

  with open("out.xml", "w") as outputFile:
      for child in root:
          out = etree.tostring(child).replace("\n","")
          outputFile.write(out+"\n")
          print(child)

# xml = open('out.xml').read()
#
# annotated_text = A(xmlCore)
# print annotated_text.sentences
# print sys.getdefaultencoding()
