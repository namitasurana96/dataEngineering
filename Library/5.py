import numpy as np
x = np.ones(15).reshape(5,3)
print("Original array:")
print(x)
print("1 on the border and 0 inside in the array")
x[1:-1,1:-1] = 0
print(x)