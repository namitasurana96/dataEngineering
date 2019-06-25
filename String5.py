String3=['ab', 'abcing', 'Stringly', 'xyz']
list=[]
for x in String3:
    list.append(x.__len__())
print(list)
print("Length of longest one:", max(list))