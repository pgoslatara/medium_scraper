import os
from pathlib import Path

import duckdb
import pyarrow as pa
import spacy


def main() -> None:
    # Ensure required directories exist
    Path(f"{os.getenv('DATA_DIR')}/enriched").mkdir(parents=True, exist_ok=True)

    nlp = spacy.load("en_core_web_sm")

    con = duckdb.connect(database=":memory:")
    df = con.execute(
        f"SELECT * FROM '{os.getenv('DATA_DIR')}/marts/dim_medium_authors.parquet'"
    ).arrow()

    # Run Spacy on all author bio's
    spacy_data = []
    for bio in df["short_bio"].to_pylist():
        extracted_data = []
        doc = nlp(bio)
        for ent in doc.ents:
            data = {
                "end_char": ent.end_char,
                "label": ent.label_,
                "start_char": ent.start_char,
                "text": ent.text,
            }
            extracted_data.append(data)
        spacy_data.append(extracted_data)

    # Assemble extracted location of each author (if exists)
    extracted_locations = []
    for author_data in spacy_data:
        loc_data = []
        for i in author_data:
            if i.get("label") == "GPE":
                loc_data.append(i["text"])
                break  # If two matching ents then we only use the first

        if loc_data == []:
            extracted_locations.append("")
        else:
            extracted_locations.append(loc_data[0])

    # Add new column to dataframe with extracted locations and save to data lake
    df = df.append_column("spacy_location", pa.array(extracted_locations, pa.string()))
    con.execute(
        f"COPY (SELECT author_url, short_bio, spacy_location FROM df) TO '{os.getenv('DATA_DIR')}/enriched/nlp_author_location.parquet' (FORMAT 'parquet');"
    )


if __name__ == "__main__":
    main()
