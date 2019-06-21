Sample= ['abc', 'xyz', 'aba', '1221']
list1=[]
for x in Sample:
    if x[0]==x[-1] and len(x)>2:
        list1.append(x)
print(list1.__len__())
