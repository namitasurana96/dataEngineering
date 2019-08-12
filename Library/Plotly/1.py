#import plotly.plotly as py
#import plotly.graph_objs as go
import numpy as np

from plotly.offline import iplot, init_notebook_mode
from plotly.graph_objs import *
import plotly.graph_objs as go
init_notebook_mode()


random_x = np.random.randn(1000)
random_y = np.random.randn(1000)
trace = go.Scatter(
    x = random_x,
    y = random_y,
    mode = 'markers'
)





data = [trace]
iplot(data, filename='basic-scatter')
