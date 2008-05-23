import ringodisco, sys

for k, v in ringodisco.ringo_reader(file(sys.argv[1]), 0, ""):
        print k, v
