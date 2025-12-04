# ======================================================================
# -*- coding: utf-8 -*-
# main.py - Core ETL pipeline for News dataset
#
# This script contains a small, modular ETL: cleaning, enrichment using an
# LLM (optional), and optional indexing (Athena). It is designed to run
# locally (using local paths) or in an environment with AWS/OpenAI SDKs.
#
# Key design goals:
# - Minimal top-level dependencies so `clean` can run without OpenAI/boto3.
# - Configurable sampling & rate limiting for safe/enabled testing of enrichment.
# - Clear CLI that supports both positional and flag-based invocation.
#
# Author: Generated/edited for repository readiness
# ======================================================================

import argparse
import json
import os
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

# Keep heavy SDK clients lazy-initialized so simple stages (e.g. `clean`)
# do not require installing OpenAI or boto3 when running locally.
OpenAI = None
client = None
boto3 = None
athena_client = None


# ---------------------------
# Global configuration
# ---------------------------
# In production, retrieve API keys from a secrets manager. For local
# testing we read from environment variables (safer than hard-coding).
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "TU_CLAVE_POR_DEFECTO_SI_NO_EXISTE")

# Categories to keep from the original dataset. This list filters Stage 1
CATEGORIES_TO_KEEP = ["WORLD NEWS", "POLITICS", "BUSINESS", "TECH", "MONEY"]

# Default batch size used during enrichment. Tune this to match available
# throughput/cost/performance constraints when calling an LLM.
BATCH_SIZE = 100


# ---------------------------
# LLM enrichment helper
# ---------------------------
def enriquecer_con_llm_ajustado(titulo: str, contenido: str):
    """Call an LLM to enrich a single article and return structured fields.

    This function is resilient and includes the following behaviors:
    - Lazy-imports the OpenAI client so the module can be imported without
      having OpenAI installed (useful for `clean`).
    - Honors `DISABLE_LLM` environment variable to return deterministic
      placeholder values for offline/test runs.
    - Returns a tuple (sentiment, category, summary). On failures it returns
      placeholder values starting with "ERROR_API" so downstream steps can
      continue.

    Args:
        titulo: Article title.
        contenido: Article content/description.

    Returns:
        tuple[str, str, str]: (sentiment, category, summary)
    """

    # Compose a strict prompt that asks the model for JSON with the fields
    # we expect. Keeping the prompt deterministic (temperature=0) reduces
    # variability in downstream parsing.
    prompt = (
        f"Analyze the following article and provide THREE data points in JSON format: "
        f"1. The sentiment, choosing ONLY one of: 'Positive', 'Negative', 'Neutral'. "
        f"2. The CATEGORY, choosing ONLY one from this list: {', '.join(CATEGORIES_TO_KEEP)}. "
        f"3. A concise summary of a maximum of 2 sentences explaining why this could be affecting the global markets. "
        f"Ensure the JSON keys are 'sentiment', 'category', and 'summary'."
        f"\nTitle: {titulo}\nContent: {contenido}"
    )

    # Test/dev shortcut: when DISABLE_LLM is set, do not perform network calls.
    if os.environ.get("DISABLE_LLM", "").lower() in ("1", "true", "yes"):
        return "ERROR_API", "ERROR_API", "LLM disabled for test run."

    # Lazy-initialize OpenAI client on first call. If the import fails we
    # log and return an error tuple so enrichment can continue without
    # raising at the module import time.
    global client, OpenAI
    if client is None:
        try:
            from openai import OpenAI as _OpenAI  # local import to avoid top-level dependency

            OpenAI = _OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
        except Exception as exc:  # pragma: no cover - depends on runtime env
            print(f"OpenAI client not available: {exc}")
            return "ERROR_API", "ERROR_API", "OpenAI client unavailable."

    # Call the model and parse the JSON response. Any error in the network
    # call or parsing returns a safe placeholder tuple.
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.0,
        )

        # The client is expected to return a JSON object string; parse it.
        llm_output = json.loads(response.choices[0].message.content)

        return (
            llm_output.get("sentiment", "N/A"),
            llm_output.get("category", "N/A"),
            llm_output.get("summary", "N/A"),
        )

    except Exception as exc:  # pragma: no cover - runtime network behavior
        print(f"Error when calling the LLM for article '{titulo[:40]}...': {exc}")
        return "ERROR_API", "ERROR_API", "Error generating summary."


# ---------------------------
# Stage 1: Extraction & Cleaning
# ---------------------------
def extract_and_clean_to_s3(input_file_path: str, s3_output_folder: str) -> str | None:
    """Read input JSONL, normalize, filter, and write a cleaned Parquet file.

    This function is intentionally simple: it focuses on structure, column
    normalization and deterministic sorting so downstream steps receive a
    consistent dataset.

    Args:
        input_file_path: Path to the input JSON lines file (local or s3-uri).
        s3_output_folder: Destination folder (local dir or s3-uri) to write Parquet.

    Returns:
        The path to the written Parquet file, or None on error.
    """

    print(f"STAGE 1: Reading data from: {input_file_path}")
    try:
        df_raw = pd.read_json(input_file_path, lines=True)
    except Exception as exc:
        print(f"Error reading input file: {exc}")
        return None

    # Normalize column names used in the dataset to a consistent schema.
    df_clean = df_raw.rename(columns={"headline": "title", "short_description": "content", "date": "publish_date"})

    # Drop rows missing essential fields and filter by categories of interest.
    df_clean = df_clean.dropna(subset=["title", "content", "category"])  # type: ignore[arg-type]
    df_clean = df_clean[df_clean["category"].isin(CATEGORIES_TO_KEEP)].copy()

    # Ensure publish_date is a proper datetime and drop invalid rows.
    df_clean["publish_date"] = pd.to_datetime(df_clean["publish_date"], errors="coerce")
    df_clean = df_clean.dropna(subset=["publish_date"])  # type: ignore[arg-type]

    # Sort chronologically and attach a stable numeric id.
    df_clean = df_clean.sort_values(by="publish_date", ascending=True).reset_index(drop=True)
    df_clean["id_news"] = df_clean.index + 1

    # Select the final output columns and write parquet. Using a timestamp
    # in the filename avoids accidental overwrites.
    df_output = df_clean[["id_news", "title", "content", "link", "publish_date", "category"]].copy()
    print(f"✅ STAGE 1: Articles cleaned and ordered: {len(df_output)}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"clean_data_{timestamp}.parquet"
    final_path = os.path.join(s3_output_folder, file_name)

    try:
        df_output.to_parquet(final_path, index=False)
        print(f"✅ STAGE 1 completed. Clean data written to: {final_path}")
        return final_path
    except Exception as exc:
        print(f"Error writing output parquet: {exc}")
        return None


# ---------------------------
# Stage 2: Enrichment (batch LLM calls)
# ---------------------------
def enrich_data_to_s3(s3_input_path: str, s3_output_folder: str, sample_size: int | None = None, rate_delay: float = 0.2) -> str | None:
    """Enrich a cleaned parquet by calling the LLM in batches and writing results.

    Important testing knobs:
    - `sample_size`: If provided, sample this many rows before processing. Useful
      to validate logic locally without processing the entire dataset.
    - `rate_delay`: Seconds to sleep between LLM calls. Use 0 to disable delays.

    Args:
        s3_input_path: Path to the cleaned parquet file to read.
        s3_output_folder: Destination folder to write enriched parquet.
        sample_size: Optional integer sample size for testing.
        rate_delay: Seconds to wait between LLM calls.

    Returns:
        Path to the enriched parquet file or None if there was an error.
    """

    df_results_final = pd.DataFrame()

    # Read the input parquet. Keep the error handling defensive so a failure
    # here won't raise at import time in other environments.
    try:
        print(f"STAGE 2: Reading clean data from: {s3_input_path}")
        df_limpio = pd.read_parquet(s3_input_path)
    except Exception as exc:
        print(f"Error reading Parquet file: {exc}")
        return None

    # Optional sampling to speed up local tests.
    if sample_size is not None:
        try:
            sample_size = int(sample_size)
            if sample_size < len(df_limpio):
                print(f"Sampling {sample_size} articles from {len(df_limpio)} total for test run.")
                df_limpio = df_limpio.sample(n=sample_size, random_state=42).reset_index(drop=True)
        except Exception as exc:
            print(f"Invalid sample_size provided: {exc}")

    # Compute batch partitions and iterate deterministically.
    n_batches = (len(df_limpio) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Starting enrichment in {n_batches} batches.")

    for i in range(n_batches):
        start_index = i * BATCH_SIZE
        end_index = min((i + 1) * BATCH_SIZE, len(df_limpio))

        batch_df = df_limpio.iloc[start_index:end_index].copy()
        print(f"  -> Processing Batch {i + 1}/{n_batches} (Articles {start_index} to {end_index - 1})...")

        results_llm_batch: list[dict] = []
        for _, row in batch_df.iterrows():
            sentiment, category, summary = enriquecer_con_llm_ajustado(row["title"], row["content"])
            results_llm_batch.append({
                "id_news": row["id_news"],
                "sentiment_llm": sentiment,
                "category_llm": category,
                "summary_llm": summary,
            })

            # Respect rate limiting when provided; set to 0 for fast local tests.
            if rate_delay and rate_delay > 0:
                time.sleep(rate_delay)

        # Merge batch results back to the original batch rows and accumulate.
        df_batch_results = pd.DataFrame(results_llm_batch)
        df_processed_batch = pd.merge(batch_df, df_batch_results, on="id_news", how="left")
        df_results_final = pd.concat([df_results_final, df_processed_batch], ignore_index=True)

    # Final formatting and write to parquet.
    df_results_final = df_results_final.rename(columns={"summary_llm": "market_impact_summary"})
    df_results_final["etl_processing_time"] = datetime.now()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"final_enriched_data_{timestamp}.parquet"
    final_path = os.path.join(s3_output_folder, file_name)

    try:
        df_results_final.to_parquet(final_path, index=False)
        print(f"✅ STAGE 2 completed. Enriched data written to: {final_path}")
        return final_path
    except Exception as exc:
        print(f"Error writing enriched parquet: {exc}")
        return None


# ---------------------------
# Stage 3: Athena indexing helper
# ---------------------------
def run_athena_query(query: str, database: str) -> bool:
    """Run a SQL query in Athena and wait for it to finish.

    This helper lazily initializes a boto3 Athena client so the module can
    be used in environments without boto3 installed. The function returns
    True when the query succeeds, False otherwise.
    """

    # Use a safe fallback for the Athena results output location: if the
    # S3_ENRICHED_FOLDER isn't defined we write to a local `athena_results`
    # folder (this makes the function usable in dev/test environments).
    try:
        base_folder = os.path.dirname(S3_ENRICHED_FOLDER)
    except Exception:
        base_folder = os.getcwd()

    output_location = f"{base_folder}/athena_results/"

    global athena_client, boto3
    if athena_client is None:
        try:
            import boto3 as _boto3

            boto3 = _boto3
            athena_client = boto3.client("athena")
        except Exception as exc:  # pragma: no cover - runtime environment
            print(f"boto3/Athena client not available: {exc}")
            return False

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_execution_id = response["QueryExecutionId"]

    # Poll for completion; a real implementation might use exponential backoff
    # and additional error handling depending on SLA requirements.
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    return status == "SUCCEEDED"


def index_data_in_athena(database_name: str, table_name: str) -> bool:
    """Trigger an Athena `MSCK REPAIR TABLE` to refresh partitions.

    Typically used after new data files are added to an S3 partitioned
    location so the Glue/Athena catalog becomes aware of the new files.
    """

    print(f"STAGE 3: Starting Indexing for {table_name} in Athena...")
    maintenance_query = f"MSCK REPAIR TABLE {table_name}"

    if run_athena_query(maintenance_query, database_name):
        print(f"✅ STAGE 3 (Indexing) completed. {table_name} is up to date.")
        return True
    else:
        print("❌ STAGE 3 failed: error running indexing in Athena.")
        return False


# ---------------------------
# Orchestration / CLI
# ---------------------------
if __name__ == "__main__":
    # The CLI supports both a short positional form and a more explicit
    # flag-based form. Examples:
    #   python main.py clean input.json ./local_clean_output/
    #   python main.py enrich --arg1 input.parquet --arg2 ./local_enriched_output --sample 100 --rate 0.0

    # Default variables the script expects to set
    stage = None
    arg1 = None
    arg2 = None

    # Fast-path positional parsing for convenience: <stage> <arg1> <arg2>
    if len(sys.argv) >= 4 and sys.argv[1] in ("clean", "enrich", "load"):
        stage = sys.argv[1]
        arg1 = sys.argv[2]
        arg2 = sys.argv[3]
    else:
        parser = argparse.ArgumentParser(description="News ETL pipeline.")
        parser.add_argument("stage", type=str, help="Stage to run: clean, enrich, or load")
        parser.add_argument("--arg1", type=str, required=True, help="Primary argument (input path or DB name)")
        parser.add_argument("--arg2", type=str, required=True, help="Secondary argument (output path or table name)")
        parser.add_argument("--sample", type=int, default=None, help="(Optional) number of articles to sample when running enrich")
        parser.add_argument("--rate", type=float, default=0.2, help="(Optional) seconds to wait between LLM calls")

        args = parser.parse_args()
        stage = args.stage
        arg1 = args.arg1
        arg2 = args.arg2
        sample = args.sample
        rate = args.rate

    # Execute the requested stage. For `enrich` we support both flag-based
    # sample/rate and extra positional args (for convenience).
    if stage == "clean":
        extract_and_clean_to_s3(arg1, arg2)

    elif stage == "enrich":
        if "sample" not in locals():
            sample = None
        if "rate" not in locals():
            rate = 0.2

        # Positional overrides (if provided): python main.py enrich <in> <out> <sample> <rate>
        if len(sys.argv) >= 5 and sys.argv[1] == "enrich":
            try:
                sample = int(sys.argv[4])
            except Exception:
                pass
        if len(sys.argv) >= 6 and sys.argv[1] == "enrich":
            try:
                rate = float(sys.argv[5])
            except Exception:
                pass

        enrich_data_to_s3(arg1, arg2, sample_size=sample, rate_delay=rate)

    elif stage == "load":
        index_data_in_athena(arg1, arg2)

    else:
        print(f"Unknown stage: {stage}")
        sys.exit(1)