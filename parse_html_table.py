import sys
import os
from lxml import etree

def parse_table(page_txt):
    '''
    Parse replicate meta op table
    '''
    html = etree.HTML(page_txt)
    tvec = html.xpath('//table/tr')
    wd_table = []
    table_text = []
    for i, tr in enumerate(tvec):
      if len(tr) and tr[0].text:
        print(tr[0].text)
        if "Begin" in tr[0].text:
          for t in tr:
            print("%d %s %s" % (i, t.tag, t.text))
          print "------"
          tr = tvec[i+1]
          for i, t in enumerate(tr):
            txt=[]
            print("%d %s %s" % (i, t.tag, t.text))
            for ttext in t.itertext():
              print("[%s]" % ttext)
              txt.extend(ttext.split('\n'))
            '''
            for j, p in enumerate(t):
              print("%d %s text <%s>" % (j, p.tag, p.text))
              for ptext in p.itertext():
                print("%s <%s>" % (p.tag, ptext))
                txt.extend(ptext.split('\n'))
              for k, b in enumerate(p):
                print("%d %s text <%s>" % (k, b.tag, b.text))
                for btext in b.itertext():
                  print("%s <%s>" % (b.tag, btext))
                  txt.extend(btext.split('\n'))
                for x in b:
                  print("--%s %s" % (x.tag, x.text))
                  for xtext in x.itertext():
                    print("--%s" % xtext)
             '''
            table_text.append(txt)
        else:
          continue
    return table_text

def main():
   f = open("./table.html", "r")
   t = parse_table(f.read())
   '''
      #>>> for i in range(90):
      #...   print "<%-20s> <%-20s> <%-40s>" % (t[0][i], t[1][i], t[2][i])
   '''
   return t


if __name__ == "__main__":
   main()