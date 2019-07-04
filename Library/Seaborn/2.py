import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

data_url = "https://raw.githubusercontent.com/resbaz/r-novice-gapminder-files/master/data/gapminder-FiveYearData.csv"
data = pd.read_csv(data_url)
sb.pointplot(x="continent", y="lifeExp", data=data)
#sb.boxplot(x="continent", y="lifeExp", data=data)

plt.show()