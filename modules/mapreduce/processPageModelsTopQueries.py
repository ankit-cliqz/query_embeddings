__author__ = 'ankit'
#from mrjob.job import MRJob
#from mrjob.step import MRStep
#import json

import json
import sys
import re
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class MRProcessPageModels(MRJob):
    SORT_VALUES = True
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, key, line):
        try:
            component = line.split("\t")
            key_tmp = component[0].strip()
            key = key_tmp[1:len(key_tmp)-1]
            value= component[1].strip()

            if key.startswith("u:"):
                self.increment_counter('mapper', 'num_inputs', 1)
                response_json = json.loads(value.decode("utf-8"))

                if (not response_json.get("tq") is None):
                    if (not response_json["tq"].get("ucrawl") is None):
                        self.increment_counter('mapper', 'ucrawl', 1)
                        ucrawl_lists = response_json["tq"]["ucrawl"]
                        if len(ucrawl_lists) > 0:
                            if len(ucrawl_lists) > 1:
                                ucrawl_lists.sort(key=lambda tup: tup[1])
                                ucrawl_lists.reverse()
                            count1 = 0
                            for item1 in ucrawl_lists:
                                if count1 == 5  and len(ucrawl_lists) > 3:
                                    break
                                self.increment_counter('mapper', 'ucrawl_queries', 1)
                                count1+=1
                                yield item1[0].encode("utf-8").strip(), None


                    if (not response_json["tq"].get("qs") is None):
                        self.increment_counter('mapper', 'qs', 1)
                        qs_lists = response_json["tq"]["qs"]
                        if len(qs_lists) > 0:
                            if len(qs_lists) > 1:
                                qs_lists.sort(key=lambda tup: tup[1])
                                qs_lists.reverse()
                            count2 = 0
                            for item2 in qs_lists:
                                if count2 == 3 and len(qs_lists) > 3:
                                    break
                                self.increment_counter('mapper', 'qs_queries', 1)
                                count2+=1
                                yield item2[0].encode("utf-8").strip(), None


                    if (not response_json["tq"].get("qc") is None):
                        self.increment_counter('mapper', 'qc', 1)
                        qc_lists = response_json["tq"]["qc"]
                        if len(qc_lists) > 1:
                            if len(qc_lists) > 1:
                                qc_lists.sort(key=lambda tup: tup[1])
                                qc_lists.reverse()
                            count3 = 0
                            for item3 in qc_lists:
                                if count3 == 3 and len(qc_lists) > 3 :
                                    break
                                self.increment_counter('mapper', 'qc_queries', 1)
                                count3+=1
                                yield item3[0].encode("utf-8").strip(), None
        except:
            self.increment_counter('mapper', 'errors', 1)
            sys.stderr.write(line)

    def reducer(self, text, data):
        yield None, text

if __name__ == '__main__':
    MRProcessPageModels.run()
