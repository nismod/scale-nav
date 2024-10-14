from numpy import log
# utils.py

# H3 resolution boundaries
res_upper_limit = 13
res_lower_limit = 3

# grid children to parent ratio
child_num=7

# earth radius
earth_radius_meters = 6378137.0

# resolution to size convertion parameters
alpha = 5/log(1_000)
A = 13 + alpha*log(10)
