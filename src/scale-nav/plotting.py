# plotting.py
from numpy import max,nan_to_num
# from pypalettes import 

def cmap(input, palette) -> list[int]:
    
    input = nan_to_num(input).tolist()
    m = max(input)
    l = palette.N
    print(f"Max input : {m:.2f}, palette colors : {l}")
    return [[int(255*j) for j in palette(int(x/m*l))] for x in input]



