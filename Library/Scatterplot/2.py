import matplotlib.pyplot as plt
import numpy as np
x = np.random.randn(5)
y = np.random.randn(5)
plt.scatter(x, y, s=700, facecolors='none', edgecolors='g')
plt.xlabel("X")
plt.ylabel("Y")
plt.show()