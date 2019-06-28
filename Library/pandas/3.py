import pandas as pd
ds1= pd.Series([1, 2, 3, 4])
ds2= pd.Series([4, 1 ])
ds=ds1+ds2
print(ds)
ds=ds1-ds2
print(ds)