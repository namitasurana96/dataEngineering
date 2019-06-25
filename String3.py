Sample= 'restart'
char=Sample[0]
Expected=Sample.replace("r",'$')
Expected=char+Expected[1:]
print(Expected)