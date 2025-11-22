import re
import pandas as pd
from typing import List
from config import EXCLUDE_ISO3


ISO_ALIASES = {
    # country-name fixes to ISO3 (extend as needed)
    "Congo, Dem. Rep.": "COD",
    "Congo, Rep.": "COG",
    "CA'te d'Ivoire": "CIV",
    "Kyrgyz Republic": "KGZ",
    "Egypt, Arab Rep.": "EGY",
    "Gambia, The": "GMB",
}


def year_columns(df: pd.DataFrame) -> List[str]:
    """Return columns that look like year values (YYYY)."""
    return [c for c in df.columns if re.fullmatch(r"\d{4}", str(c))]


def to_numeric_series(s: pd.Series) -> pd.Series:
    """Convert a series to numeric, stripping common thousand/nbsp characters."""
    return pd.to_numeric(
        s.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("\u00a0", "", regex=False),
        errors="coerce",
    )


def exclude_israel(df: pd.DataFrame, iso_col: str = "iso3") -> pd.DataFrame:
    """Exclude rows where iso_col is in the configured EXCLUDE_ISO3 set."""
    s = df.get(iso_col, pd.Series(index=df.index, dtype=object))
    return df[~s.isin(EXCLUDE_ISO3)].copy()


def profile_block(df: pd.DataFrame, name: str) -> str:
    """Small textual profile block for a dataframe."""
    lines = [f"### {name}", f"- rows: {len(df):,}"]
    if "year" in df.columns:
        yrs = df["year"].dropna()
        if not yrs.empty:
            lines.append(
                f"- years: {int(yrs.min())}-{int(yrs.max())} "
                f"(distinct={yrs.nunique():,})"
            )
    if "iso3" in df.columns:
        lines.append(f"- countries: {df['iso3'].dropna().nunique():,}")
    top_nulls = df.isna().mean().sort_values(ascending=False).head(6)
    lines.append(
        "- top null ratios: "
        + ", ".join([f"{k}={v:.1%}" for k, v in top_nulls.items()])
    )
    return "\n".join(lines)

