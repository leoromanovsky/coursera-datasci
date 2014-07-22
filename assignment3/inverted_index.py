import MapReduce
import sys

"""
Create an inverted index.
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    document_id = record[0]
    text = record[1]
    words = text.split()
    words = [x.strip() for x in words]

    unique_words = set(words)

    for w in unique_words:
      mr.emit_intermediate(w, document_id)

def reducer(key, document_ids):
    mr.emit((key, document_ids))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
