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
    person = record[0]
    friend = record[1]

    mr.emit_intermediate((person, friend), (person, friend))
    mr.emit_intermediate((friend, person), (person, friend))

def reducer(person, friend_pairs):
    #print person
    #print friend_pairs

    if (len(friend_pairs) == 1) and (person == friend_pairs[0]):
      mr.emit((person[0], person[1]))
      mr.emit((person[1], person[0]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
