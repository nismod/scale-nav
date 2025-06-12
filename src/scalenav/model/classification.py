import re
import itertools as iter
import numpy as np

# data
import pandas as pd

# import shapely
from scalenav.plotting import cmap

import ibis as ib
from ibis import _

ib.options.interactive = True

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

#####

light_lm = "all-MiniLM-L6-v2"
full_lm = "all-mpnet-base-v2"

selected_model = full_lm

model = SentenceTransformer(selected_model)


class ReferenceData(pd.DataFrame):
    def __init__():
        pass

    # enforce a structure for the data here for better usability ?


### Embedding and matching function
def embedding_match_category(category, reference_data):
    poi_embedding = model.encode(category)
    embeddings = list(reference_data["embedding"])
    similarities = cosine_similarity([poi_embedding], embeddings)[0]
    best_match_index = similarities.argmax()
    best_match_score = similarities[best_match_index]
    best_match_row = reference_data.iloc[best_match_index]

    return [
        best_match_row["section"],
        best_match_row["Code"],
        best_match_row["Description"],
        best_match_score,
    ]


def classify(data, reference_data):
    # Precompute embeddings for ISIC categories
    reference_data["embedding"] = reference_data["detailed_descr"].apply(
        lambda x: model.encode(x)
    )
