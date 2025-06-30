from collections import defaultdict
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from typing import List

import time

def search_entities_in_vdb_core(names, embedding_model, combined_entities):
    t0 = time.time()

    results = []
    top_k = 5
    similarity_threshold = 0.7

    def brand_group_factory():
        return {
            "type": None,
            "score": 0,
            "metadata": {
                "categories": [],
                "source_table": None,
                "column": None
            },
        }

    for name in names:
        query_emb = np.array(
            embedding_model.embeddings.create(input=[name], model="text-embedding-3-large").data[0].embedding
        )

        all_embeddings = np.vstack(combined_entities["embedding"])
        sims = cosine_similarity([query_emb], all_embeddings)[0]

        top_indices = np.argsort(sims)[::-1]
        filtered_matches = [idx for idx in top_indices[:top_k * 2] if sims[idx] >= similarity_threshold][:top_k]

        grouped_brands = defaultdict(brand_group_factory)
        plain_matches = []

        for idx in filtered_matches:
            row = combined_entities.iloc[idx]
            matched_name = row["name"]
            score = round(sims[idx], 4)
            row_meta = row["metadata"]

            if row["type"] == "Brand_Name":
                g = grouped_brands[matched_name]
                g["type"] = row["type"]
                g["score"] = max(g["score"], score)
                cat = row_meta.get("Category_Name")
                if cat and cat not in g["metadata"]["categories"]:
                    g["metadata"]["categories"].append(cat)
                g["metadata"]["source_table"] = row_meta.get("source_table")
                g["metadata"]["column"] = row_meta.get("column")
            else:
                plain_matches.append({
                    "matched_name": matched_name,
                    "type": row["type"],
                    "score": score,
                    "metadata": row_meta,
                })

        merged_brand_matches = [
            {
                "matched_name": m_name,
                "type": data["type"],
                "score": data["score"],
                "metadata": data["metadata"],
            }
            for m_name, data in grouped_brands.items()
        ]

        matches = merged_brand_matches + plain_matches
        matches.sort(key=lambda m: m["score"], reverse=True)

        brand_matches = [entity for entity in matches if entity["type"] == "Brand_Name"]
        item_matches = [entity for entity in matches if entity["type"] == "Item_Name"]
        other_matches = [entity for entity in matches if entity["type"] not in {"Brand_Name", "Item_Name"}]

        if brand_matches and item_matches:
            top_brand_score = brand_matches[0]["score"]
            top_item_score = max(entity["score"] for entity in item_matches)

            if top_brand_score >= top_item_score:
                matches = brand_matches + other_matches
            else:
                matches = item_matches + other_matches

        matches.sort(key=lambda m: m["score"], reverse=True)
        results.append({"original": name, "matches": matches})

    t1 = time.time()
    print(f"🕒 Tool took {t1 - t0:.2f} seconds")

    return results
