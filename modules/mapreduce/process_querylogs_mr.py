__author__ = 'ankit'
#from mrjob.job import MRJob
#from mrjob.step import MRStep
#import json

import json
import sys
import re
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class MRProcessQueryLogs(MRJob):
    SORT_VALUES = True
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, key, line):
        try:
            component = line.split("\t")
            if component[1]:
                self.increment_counter('mapper', 'num_inputs', 1)
                response_json = json.loads(component[1].decode("utf-8"))
                languageFlag = False
                if (not response_json.get("snippet") is None) and (not response_json.get("language") is None):
                    language_dict = response_json["language"]
                    for language, score in language_dict.iteritems():
                        if (language.encode("utf-8") == "de" or language.encode("utf-8") == "en"):
                            languageFlag = True
                            self.increment_counter('mapper', 'language', 1)
                            break

                    if (languageFlag == True) and (not response_json["snippet"].get("og") is None):
                        title = ""
                        description = ""

                        if (not response_json["snippet"]["og"].get("title") is None):
                            title = response_json["snippet"]["og"]["title"].encode("utf-8").strip()
                            self.increment_counter('mapper', 'title', 1)

                        if (not response_json["snippet"]["og"].get("description") is None):
                            description = response_json["snippet"]["og"]["description"].encode("utf-8").strip()
                            self.increment_counter('mapper', 'description', 1)
                        output = title.strip() + "  " + description.strip()
                        self.increment_counter('mapper', 'num_outputs', 1)
                        yield output, None
        except:
            self.increment_counter('mapper', 'errors', 1)
            sys.stderr.write(line)

    def reducer(self, text, data):
        yield None, text

# if __name__ == '__main__':
#     SyncLastFetched.run()

#
# class MRProcessQueryLogs(MRJob):
#     def mapper_process(self, _, line):
#         component = line.split("\t")
#         if component[1]:
#             response_json = json.loads(component[1].decode("utf-8"))
#             languageFlag=False
#             if (not response_json.get("snippet") is None) and (not response_json.get("language") is None):
#                 language_dict  = response_json["language"]
#                 for language, score in language_dict.iteritems():
#                     if (language.encode("utf-8")=="de" or language.encode("utf-8")=="en"):
#                         languageFlag =True
#                         break
#
#                 if (languageFlag==True) and (not response_json["snippet"].get("og") is None):
#                     title = ""
#                     description = ""
#
#                     if (not response_json["snippet"]["og"].get("title") is None):
#                         title = response_json["snippet"]["og"]["title"].encode("utf-8").strip()
#
#                     if (not response_json["snippet"]["og"].get("description") is None):
#                         description = response_json["snippet"]["og"]["description"].encode("utf-8").strip()
#                     output = title.strip()+"  "+description.strip()
#                     yield output, None
#
#
#  #   def combiner_process(self, text, value):
#  #       yield text, value
#
#     def reducer_process(self, text, _):
#         yield text, None
#
#     def steps(self):
#         return [MRStep(mapper=self.mapper_process,
#                        reducer=self.reducer_process)]
if __name__ == '__main__':
    MRProcessQueryLogs.run()
