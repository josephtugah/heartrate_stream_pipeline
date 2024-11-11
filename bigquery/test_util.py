def hello(name):
  print("hello {}".format(name))

def read_lines(path):
  with open(path) as f:
    return f.readlines()
