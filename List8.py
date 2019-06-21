List=['abc', 'cdesf', 'pqrast', 'mnopqs']
n=5
for x in List:
    if len(x)>n:
        print("length of", x,":",len(x),"is greater than ",n)
    else:
        print("length of", x,":",len(x), "is less than ",n)