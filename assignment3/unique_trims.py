import MapReduce
import sys

"""
Find asymmetric relationships.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    sequence_id = record[0]
    nucleotides = record[1]

    length = len(nucleotides)
    trimmed = nucleotides[0:length-10]

    mr.emit_intermediate(trimmed, 1)

def reducer(nucleo, foo):
    mr.emit(nucleo)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
