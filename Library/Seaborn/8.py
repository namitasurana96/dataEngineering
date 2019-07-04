import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

data_url = "https://raw.githubusercontent.com/sudhakarpatil/seaborn-data/master/tips.csv"
data = pd.read_csv(data_url)
print(data)
sb.swarmplot(x="size", y="total_bill", data=data)
plt.show()