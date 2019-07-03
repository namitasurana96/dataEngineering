import matplotlib.pyplot as plt
from pylab import randn
X = randn(5)
Y = randn(5)
z = randn(5)
size = range(10,20,2)
S = [value * 3 for value in size]

plt.scatter(X, Y, S,c='red')
print(*size)
plt.xlabel("X")
plt.ylabel("Y")
print(X)
print(Y)
plt.show()