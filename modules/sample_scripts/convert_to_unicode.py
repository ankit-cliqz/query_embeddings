__author__ = 'ankit'


#with open('') as f1:
#    		for line1 in f1:

str = u"des [[R\u00f6misch-deutscher K\u00f6nig|deutschen K\u00f6nigs]] und sp\u00e4teren Kaisers [[Maximilian I. (HRR)|Maximilian I.]] Zwar war das Rennen schon seit mehr als einem Jahrhundert eine bekannte Turniervariante, die vor allem bei jungen Adligen beliebt war, doch eine Standardisierung der Ausr\u00fcstung kam erst unter Maximilians Leitung zustande. In seiner sp\u00e4ten, sportlichen "

#z = unicodedata.normalize('NFKD', str).encode('ascii', 'ignore')

z = str.encode("utf-8")
print z
