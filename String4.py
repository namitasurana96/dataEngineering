String3={'ab', 'abc', 'String', 'vf', 'xyz'}
list=[]
for x in String3:
    if len(x)<3:
        list.append(x)
    elif x[-1]== 'g':
        x=x+'ly'
        list.append(x)
    else:
        x=x+'ing'
        list.append(x)
print(list)

