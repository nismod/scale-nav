# plotting.py
from numpy import max,nan_to_num
# from pypalettes import 

def cmap(input, palette):
    input = nan_to_num(input).tolist()
    m = max(input)
    l = palette.N
    print("Max input : {}, palette colors : {}".format(m,l))
    return [[int(255*j) for j in palette(int(x/m*l))] for x in input] 



