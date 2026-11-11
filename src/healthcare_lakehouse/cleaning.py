r"""Rule C1: text cleaning with explicit character sets.

Spark's ``trim()`` and Java's ``\s`` do not match the no-break space U+00A0, and Python's ``\s`` does.
The steps below spell out the character set so that Spark and the pandas reference build agree.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

NO_BREAK_SPACE = "\u00a0"


def clean_text(col: Column) -> Column:
    """Apply rule C1 to one text column, in order.

    1. U+00A0 becomes a space and a backtick becomes an apostrophe (``VALLE D`AOSTA`` -> ``VALLE D'AOSTA``).
    2. Runs of space, tab, CR or LF become one space.
    3. Leading and trailing spaces are trimmed.
    4. An empty string becomes null. The original casing is kept.
    """
    translated = F.translate(col, NO_BREAK_SPACE + "`", " '")
    collapsed = F.regexp_replace(translated, "[ \t\r\n]+", " ")
    trimmed = F.trim(collapsed)
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)


def clean_text_columns(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Return ``df`` with rule C1 applied to each of ``columns``."""
    return df.withColumns({name: clean_text(F.col(name)) for name in columns})
