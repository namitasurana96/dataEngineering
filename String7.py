string = input('Enter the comma separated string -> ')


get_list = list(string.split(','))
get_list.sort()
print(get_list)
sets= set(get_list)
print(sets)