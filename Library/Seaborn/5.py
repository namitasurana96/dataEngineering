import matplotlib.pyplot as plt
import seaborn as sb

df = sb.load_dataset("iris")
sb.violinplot(x="species", y="sepal_length", data=df)
plt.show()


