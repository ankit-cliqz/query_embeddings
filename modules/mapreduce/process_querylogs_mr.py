__author__ = 'ankit'
from mrjob.job import MRJob
from mrjob.step import MRStep
import json
class MRProcessQueryLogs(MRJob):
    def mapper_process(self, _, line):
        component = line.split("\t")
        if component[1]:
            response_json = json.loads(component[1].decode("utf-8"))
            languageFlag=False
            if (not response_json.get("snippet") is None) and (not response_json.get("language") is None):
                language_dict  = response_json["language"]
                for language, score in language_dict.iteritems():
                    if (language.encode("utf-8")=="de" or language.encode("utf-8")=="en"):
                        languageFlag =True
                        break

                if (languageFlag==True) and (not response_json["snippet"].get("og") is None):
                    title = ""
                    description = ""

                    if (not response_json["snippet"]["og"].get("title") is None):
                        title = response_json["snippet"]["og"]["title"].encode("utf-8").strip()

                    if (not response_json["snippet"]["og"].get("description") is None):
                        description = response_json["snippet"]["og"]["description"].encode("utf-8").strip()
                    output = title+"  "+description
                    yield _,output


    def combiner_process(self, _, text):
        yield _, text

    def reducer_process(self, _, text):
        yield _, text

    def steps(self):
        return [MRStep(mapper=self.mapper_process,
                       combiner=self.combiner_process,
                       reducer=self.reducer_process)]
if __name__ == '__main__':
    MRProcessQueryLogs.run()