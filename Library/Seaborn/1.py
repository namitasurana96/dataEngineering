import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt
from urllib.request import urlopen

# with open("titanic.csv") as f:
# data = f.read()
# print(data)

# link = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/titanic.csv"
# with urlopen(link) as f:
# # f = urlopen(link)
# data = f.read().decode('utf-8')
# data = data.split('\n')

'# to display all table data in row column format'
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 1000)

'# load_dataset function automatically fetched data from internet'
df = sb.load_dataset('titanic')
# print(df.loc[df["sex"] == "female", "survived"].mean())
sb.barplot(x="sex", y="survived", data=df, hue="class")
plt.title("Sea born Bar plot")
plt.show()