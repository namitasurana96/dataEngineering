d = {'a':2, 'b':23, 'c':5, 'd':17, 'e':1}
items = [(k, v) for v, k in d.items()]
items.sort()
print(items)
items.reverse()
#items = [(k, v) for v, k in items]
print(items)