'''
import numpy as np
x = np.ones((3,3))
print("Original array:")
print(x)
print("0 on the border and 1 inside in the array")
x = np.pad(x, pad_width=1, mode='constant', constant_values=0)
print(x)



from matplotlib import pyplot as plt
plt.plot([1,5,9],[2,3,6])
plt.show()
#plt.savefig("img1.png", bbox_inches='tight')
'''


import matplotlib
from matplotlib import pyplot as plt
slices=[7,12,5,41]
activities=['sleeping', 'eating', 'working', 'playing']
cols=['c', 'r', 'm', 'b']

plt.pie(slices, labels=activities, colors=cols, startangle=40, autopct='%1.1f%%')
plt.title('Pie Plot')
plt.show()
