from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    explode,
    length,
    lower,
    regexp_replace,
    split,
    trim,
    window,
)


def build_trending_keywords_df(enriched_df: DataFrame) -> DataFrame:
    keywords_df = (
        enriched_df.select(
            col("event_time"),
            explode(split(col("title"), r"\s+")).alias("raw_keyword"),
        )
        .withColumn(
            "keyword",
            trim(
                regexp_replace(
                    lower(col("raw_keyword")),
                    r"^[^\w]+|[^\w]+$",
                    "",
                )
            ),
        )
        .filter(length(col("keyword")) > 3)
        .drop("raw_keyword")
    )

    return (
        keywords_df.groupBy(
            window(col("event_time"), "1 hour", "15 minutes"),
            col("keyword"),
        )
        .agg(count("*").alias("frequency"))
    )
