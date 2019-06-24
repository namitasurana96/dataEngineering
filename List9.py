def common(list1, list2):
    result = False
    for x in list1:
        for y in list2:
            if x == y:
                result = True
                return result
    return result


# driver code
a = [1, 2, 3, 4, 10]
b = [5, 6, 7, 8, 9]
print(common(a, b))