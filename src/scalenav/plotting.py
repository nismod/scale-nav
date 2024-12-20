# plotting.py
from numpy import max,nan_to_num,log1p
from pandas import Series
from matplotlib.colors import LinearSegmentedColormap,ListedColormap
from pypalettes import load_cmap
# from pypalettes import 

def cmap(input : [Series,list], palette : (LinearSegmentedColormap | ListedColormap | str), log : bool = False, pydeck : bool = True, factorize : bool = False) -> list[int]: # type: ignore
    """Provide colormap for deckgl plotting.

    Parameters
    ----------

    input : a list or pandas.Series of numeric values

    palette : a palette from matplotlib.colors, pypalettes, or a palette name from https://python-graph-gallery.com/color-palette-finder/. 

    log : whether the scale should be logarithmic

    pydeck : whether or not the colors are for pydeck maps. If true, the returned color values are integers between 0 and 255 instead of floats between 0 and 1.
    
    Returns
    ---------
    a list containting lists of length 3 with r,g,b values between 0 and 255 for each value from input. 
    """
    #  if factorize: can be done outside of the function as well. Just provide a different color palette. 

    if log:
        input = nan_to_num(log1p(input)).tolist()
    else:
        input = nan_to_num(input).tolist()

    if palette is str:
        palette = load_cmap(palette)

    m = max(input)
    l = palette.N

    print(f"Max input : {m:.2f}, palette colors : {l}")
    
    if pydeck:
        return [[int(255*j) for j in palette(int(x/m*l))] for x in input]
    else : 
        return [palette(int(x/m*l)) for x in input]
    



