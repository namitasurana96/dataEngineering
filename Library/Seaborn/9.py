
import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd

dataurl="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
data = pd.read_csv(dataurl)
print(data)
sb.swarmplot(x="day", y="total_bill", data=data)
plt.show()