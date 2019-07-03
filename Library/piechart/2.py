import matplotlib.pyplot as plt
languages = 'Java', 'Python', 'PHP', 'JavaScript', 'C#', 'C++'
popularity = [22.2, 17.6, 8.8, 8, 7.7, 6.7]
colors = ['red', 'gold', 'yellowgreen', 'blue']
explode = (0.1, 0, 0, 0, 0, 0)
# Plot
plt.pie(popularity, explode=explode, labels=languages, colors=colors,
autopct='%1.1f%%', shadow=True, startangle=140)
plt.title("Popularity of Programming Language")
plt.show()