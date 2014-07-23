import MapReduce
import sys

"""
Count the number of friends each person has.
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    person = record[0]
    friend = record[1]

    mr.emit_intermediate(person, friend)

def reducer(person, friends):
    total = len(friends)
    mr.emit((person, total))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
