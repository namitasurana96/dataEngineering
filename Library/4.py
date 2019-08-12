import numpy as np
x = np.arange(12, 37).reshape(5,5)
print("Original array:")
print(x)
print("Reverse array:")
x = x[::-1]
print(x)