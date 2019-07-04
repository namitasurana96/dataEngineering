import matplotlib.pyplot as plt
import seaborn as sb

df = sb.load_dataset("tips")
sb.violinplot(x="sex", y="total_bill", data=df)
plt.show()