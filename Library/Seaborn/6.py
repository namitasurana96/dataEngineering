import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd

data_url = "https://raw.githubusercontent.com/resbaz/r-novice-gapminder-files/master/data/gapminder-FiveYearData.csv"
data = pd.read_csv(data_url)

sb.boxplot(x="continent" , y="lifeExp" , data=data)
plt.show()
