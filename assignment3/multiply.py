import MapReduce
import sys

"""
Matrix multiplication.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    matrix, i, j, value = record

    for k in range(0, 5):
      if matrix == 'a':
        key = (i, k)
        mr.emit_intermediate(key, record)
      else:
        key = (k, j)
        mr.emit_intermediate(key, record)

def reducer(position, values):
    i, j = position

    product = 0
    temp = {}
    for v in values:
      if v[0] == 'a':
        join_index = v[2]
      else:
        join_index = v[1]

      if join_index in temp:
        product = product + temp[join_index] * v[3]
      else:
        temp[join_index] = v[3]

    mr.emit((i, j, product))


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
