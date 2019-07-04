import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd

dataurl="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
data=pd.read_csv(dataurl)

sb.boxplot(x="tip", y="day" , data=data)
plt.show()