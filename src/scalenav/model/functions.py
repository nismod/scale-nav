from scipy.spatial.distance import jensenshannon
from sklearn.preprocessing import normalize
from sklearn.metrics.pairwise import cosine_similarity
from numpy import all, log, count_nonzero


def JS_div(p, q, norm="l1"):
    if (len(p) == 1) & (len(q) == 1):
        if all(p != q):
            return 1
    p = normalize([p], norm=norm)[0]
    q = normalize([q], norm=norm)[0]
    return jensenshannon(p, q, base=2)


def cos_similarity(p, q, norm="l1"):
    p = normalize([p], norm=norm)[0].reshape(1, -1)
    q = normalize([q], norm=norm)[0].reshape(1, -1)
    return cosine_similarity(p, q)[0][0]


def CrossEntropy():
    pass


def jaccard_distance(list1, list2):
    """
    Computes the Jaccard distance between two lists of strings.

    Jaccard distance = 1 - (size of intersection / size of union)

    Parameters:
    - list1, list2: Lists containing strings

    Returns:
    - A float value between 0 (identical) and 1 (completely disjoint)
    """

    set1 = set(list1)
    set2 = set(list2)

    intersection = set1 & set2
    union = set1 | set2

    if not union:
        return 0.0  # Define distance as 0 if both sets are empty

    return 1 - len(intersection) / len(union)


def spat_entropy(
    value, general_vars, secondary_vars, general_vars_S_max, secondary_vars_S_max
):

    non_zero_gen = count_nonzero(value[general_vars])

    if non_zero_gen == 0:
        general_vars_S = 0
    else:
        general_vars_S = 1 - (log(non_zero_gen) / general_vars_S_max)

    non_zero_sec = count_nonzero(value[secondary_vars])

    # if non_zero_sec==0 and non_zero_gen==0:
    #     return 0
    if non_zero_sec == 0:
        secondary_vars_S = 1
    else:
        secondary_vars_S = log(non_zero_sec) / secondary_vars_S_max

    return general_vars_S, secondary_vars_S


def information_metric(data, general_vars, secondary_vars):
    general_vars_len = len(general_vars)
    secondary_vars_len = len(secondary_vars)

    general_vars_S_max = log(general_vars_len)
    secondary_vars_S_max = log(secondary_vars_len)

    return data.apply(
        spat_entropy,
        general_vars=general_vars,
        secondary_vars=secondary_vars,
        general_vars_S_max=general_vars_S_max,
        secondary_vars_S_max=secondary_vars_S_max,
        axis=1,
        result_type="expand",
    )
