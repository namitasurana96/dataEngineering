import matplotlib.pyplot as plt
f = open("Cordnts.txt")
data = f.read()

data = data.split("\n")
data.remove(data[-1])

X = [row.split(' ')[0] for row in data]
Y = [row.split(' ')[1] for row in data]

plt.plot(X, Y)
plt.xlabel("X - Axis")
plt.ylabel("Y - Axis")
plt.title("Graph")
plt.show()