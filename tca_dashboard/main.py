import sys
sys.path.append('../')
from scripts.enhanced_dashboard import *
from scripts.neutral_dashboard import *
from scripts.slippage_dashboard import *

from bokeh.models import Tabs

zztab1,zztab2,zztab3 = enhanced_dashboard()
zxtab1,zxtab2,zxtab3,zxtab4 = neutral_dashboard()
jytab1,jytab2 = slippage_dashboard()

# Put all the tabs into one application
tabs = Tabs(tabs = [zztab1,zztab2,zztab3,zxtab1,zxtab2,zxtab3,zxtab4,jytab1,jytab2])

# Put the tabs in the current document for display
curdoc().add_root(tabs)