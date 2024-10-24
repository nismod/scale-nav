# plotting.py
from numpy import max,nan_to_num,log1p
from pandas import Series
# from pypalettes import 

def cmap(input : [Series,list], palette, log : bool = False) -> list[int]: # type: ignore
    """Provide colormap for deckgl plotting.

    Parameters
    ----------

    input : a list or pandas.Series of numeric values

    palette : a palette from pypalettes.

    log : whether the scale should be logarithmic

    Returns
    ---------

    a list containting lists of length 3 with r,g,b values between 0 and 255 for each value from input. 
    
    """

    if log:
        input = nan_to_num(log1p(input)).tolist()
    else:
        input = nan_to_num(input).tolist()
    m = max(input)
    l = palette.N
    print(f"Max input : {m:.2f}, palette colors : {l}")
    return [[int(255*j) for j in palette(int(x/m*l))] for x in input]
    



