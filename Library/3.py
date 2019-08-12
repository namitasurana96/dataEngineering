import numpy as np
x = np.zeros(9).reshape(3,3)
y=np.ones(9).reshape(3,3)
z=np.ones(5)
print(z.reshape(1))
x[1][2]=11
print(x)
print(y)