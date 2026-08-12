import sys
n = int(sys.argv[1])
SEG = ["A","B","C","D"]
w = sys.stdout.write
for i in range(n):
    w('{"id":"K%08d","name":"N-%d-0","seg":"%s","amt":%d.0,"eff":"2025-06-01T00:00:00Z"}\n'
      % (i, i, SEG[i%4], i*7 % 100000))
