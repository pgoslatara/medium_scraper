import os
from pathlib import Path
from typing import List

import duckdb
import pyarrow as pa
import spacy


def extract_author_location_using_nlp(df: pa.lib.StringArray) -> List[str]:
    nlp = spacy.load("en_core_web_sm")

    print(f"Running Spacy on {df.num_rows} authors...")

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
            extracted_locations.append(str(loc_data[0]))

    return extracted_locations


def extract_author_linkedin_username(df: pa.lib.StringArray) -> List[str]:
    print(f"Extracting LinkedIn username for {df.num_rows} authors...")

    linkedin_usernames = []
    for bio in df["short_bio"].to_pylist():
        if bio.find("linkedin.com/in/") > 0:
            linkedin_url_raw = bio[bio.find("linkedin.com/in/") :]
            linkedin_url = linkedin_url_raw[: linkedin_url_raw.find(" ")]
            linkedin_usernames.append(linkedin_url)
        else:
            linkedin_usernames.append("")

    return linkedin_usernames


def extract_author_twitter_handle(df: pa.lib.StringArray) -> List[str]:
    print(f"Extracting Twitter handle for {df.num_rows} authors...")

    twitter_handles = []
    for bio in df["short_bio"].to_pylist():
        if bio.find("twitter.com/") > 0:
            twitter_url_raw = bio[bio.find("twitter.com/") :]
            twitter_url = twitter_url_raw[: twitter_url_raw.find(" ")]
            twitter_handles.append(twitter_url)
        else:
            twitter_handles.append("")

    return twitter_handles


def main() -> None:
    # Ensure required directories exist
    Path(f"{os.getenv('DATA_DIR')}/enriched").mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    df = con.execute(
        f"SELECT * FROM '{os.getenv('DATA_DIR')}/staging/medium_authors/stg_medium_authors__authors.parquet'"
    ).arrow()

    extracted_locations = extract_author_location_using_nlp(df)
    linkedin_usernames = extract_author_linkedin_username(df)
    twitter_handles = extract_author_twitter_handle(df)

    # Add new columns to dataframe and save to data lake
    df = df.append_column("spacy_location", pa.array(extracted_locations, pa.string()))
    df = df.append_column(
        "linkedin_username", pa.array(linkedin_usernames, pa.string())
    )
    df = df.append_column("twitter_handle", pa.array(twitter_handles, pa.string()))
    dest_file_name = f"{os.getenv('DATA_DIR')}/enriched/nlp_author_location.parquet"
    print(f"Saving to {dest_file_name} ({df.num_rows} rows)...")
    con.execute(
        f"COPY (SELECT author_url, linkedin_username, short_bio, spacy_location, twitter_handle FROM df) TO '{dest_file_name}' (FORMAT 'parquet');"
    )


if __name__ == "__main__":
    main()
