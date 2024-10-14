# plotting.py
from numpy import max,nan_to_num,log1p
from pandas import Series
# from pypalettes import 

def cmap(input : [Series,list], palette, log : bool = False) -> list[int]: # type: ignore
    # input = []
    if log:
        input = nan_to_num(log1p(input)).tolist()
    else:
        input = nan_to_num(input).tolist()
    m = max(input)
    l = palette.N
    print(f"Max input : {m:.2f}, palette colors : {l}")
    return [[int(255*j) for j in palette(int(x/m*l))] for x in input]



