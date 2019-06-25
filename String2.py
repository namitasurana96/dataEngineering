str="google.com"
list={}
for x in str:
    if x in list:
        list[x]+=1
    else:
        list[x]=1
print(list)