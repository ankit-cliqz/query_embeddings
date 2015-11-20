__author__ = 'ankit'

from HTMLParser import HTMLParser
import codecs
import  json
import re
html_escape_table = {
        "&": "&amp;",
        '"': "&quot;",
        "'": "&apos;",
        ">": "&gt;",
        "<": "&lt;",
        }

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)

class ProcessHTMLContent(object):
#    def __init__(self):
#        self.regexbackslash = re.compile(r'\\(?![/u"])')

    def strip_tags(self, html):
        s = MLStripper()
        s.feed(html)
        return s.get_data()

    # We dont' use this function anywhere
    def html_escape(self,text):
        """Produce entities within text."""
        return "".join(html_escape_table.get(c,c) for c in text)


    def unescapeHtmlChars(self,s):
        s = s.replace(";lt;", "").replace(";gt;", "").replace(";amp;", "").replace(";apos;", "").replace(";quot;", "")
        return s


    def process_html_content(self,line):
        line_tmp = line.strip()
        #fixed_backslashes = self.regexbackslash.sub(r"\\\\", line_tmp)

        parser = HTMLParser()
        line = parser.unescape(self.unescapeHtmlChars(line))
        processed_output = self.strip_tags(line)+"\n"
        return processed_output.encode("utf-8")

def main():
    pc = ProcessHTMLContent()
    with codecs.open('/Users/ankit/Documents/cliqz/query_embeddings/data/top1000out', 'r') as f1:#, encoding='utf-8') as f1:
        for line in f1:
            out = pc.process_html_content(line)

    f1.close()


if __name__=="__main__":
    main()
