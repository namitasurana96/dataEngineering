import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

df = sb.load_dataset("tips")
sb.scatterplot(x="day", y="total_bill", data=df, hue="smoker")

plt.show()