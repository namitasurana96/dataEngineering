#def ml():
'''import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

dataset = pd.read_csv('GoogleSPT.csv')
print(type(dataset))
X = dataset.iloc[:, 1:2].values
y = dataset.iloc[:,-1].values

print(X)
print(type(X))
print(type(y))

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 1/3, random_state = 0)

from sklearn.preprocessing import StandardScaler
sc_X=StandardScaler()
sc_Y=StandardScaler()
X_train=sc_X.fit_transform(X_train)
X_test=sc_X.transform(X_test)
y_train=sc_Y.fit_transform(y_train)
y_test=sc_Y.transform(y_test)

from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(X_train, y_train)

y_pred = regressor.predict(X_test)
print(y_pred)
'''


import pandas as pd
import numpy as np
import apache_beam as beam
from sklearn.linear_model import LinearRegression
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions()
p = beam.Pipeline(options=options)


def printdata(value):
    print (value[1])


def slplt(value):
    value= (value.split(','))
    return value


def MLmodel(data):
    df = pd.DataFrame(data[1][1:])
    X = df.iloc[:, -1:].values
    y = df.iloc[:, 1].values
    print(df.head(), df.info())
    #print(X)
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=1 / 3, random_state=0)

    from sklearn.linear_model import LinearRegression
    regressor = LinearRegression()
    regressor.fit(X_train, y_train)

    y_pred = regressor.predict(X_test)
    return y_pred

p = beam.Pipeline('Directrunner')
lines = \
(p  | 'ReadFileFromCSV' >> beam.io.ReadFromText('Salary_Data.csv')
#   | 'Splitter using beam.Map' >> beam.Map(slplt)
    | 'Map record to key' >> beam.Map(lambda record: ('record', record.split(',')))

    | 'GroupBy data' >> beam.GroupByKey()
#   | "print data" >> beam.ParDo(printdata)
    | 'Build Model' >> beam.ParDo(MLmodel)
#   | 'Combine' >> beam.CombineValues(beam.combiners.Dict)
    | 'Write output to file' >> beam.io.WriteToText('Salaryoutput.csv')
)
p.run()