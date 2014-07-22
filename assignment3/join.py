import MapReduce
import sys

"""
Implement a relational join.
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    table = record[0]
    order_id = record[1]
    attributes = record[2:]

    mr.emit_intermediate(order_id, record)

def reducer(key, records):
    order = records[0]
    for r in records[1:]:
      mr.emit(order+r)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
