"""
Here are all the auxiliary functions being used throughout the processing pipeline.
"""
import string
from functools import partial
from typing import ClassVar, Dict

import dateparser as dp
import polars as pl

from anonimus.etc.schema import Document


def pipe_cmp_date(dx: ClassVar[Document], dy: ClassVar[Document]):
    """
    Comparator to sort the given `documents` by datetime attribute, located in `meta` subfield
    Args:
        dx (_type_): Document
        dy (_type_): Document
    """
    predate = dp.parse(dx.meta["timestamp"])
    curdate = dp.parse(dy.meta["timestamp"])
    if predate >= curdate:
        return 1
    return -1


def pipe_std_parse(d):
    time = None
    if isinstance(d, Document):
        time = d.meta["timestamp"]
    else:
        time = str(d)
    _dt = dp.parse(time)
    d, m, y = _dt.strftime("%d"), _dt.strftime("%m"), _dt.strftime("%y")
    h, m, s = _dt.strftime("%H"), _dt.strftime("%M"), _dt.strftime("%S")
    return f"{d}/{m}/{y} {h}:{m}:{s}"


def pipe_is_null(x):
    return x == pl.Null() or str(x).strip() == ""


def std_map(mapping: Dict):
    cursor = pl.element()
    for pre, nex in mapping.items():
        cursor = cursor.str.replace_all(pre, str(nex), literal=True)
    return cursor


# TODO: add normal form checking
def std_replace(x: str, wordlist):
    r = []
    for w in x.split(" "):
        w = w.strip()
        _w = "".join([ch for ch in w if ch not in string.punctuation])
        if any([str(ch).isdigit() for ch in _w]):
            mask = "".join([str(ch) if not ch.isdigit() else "*" for ch in _w])
            r.append(mask)
        elif _w.strip().lower() not in wordlist:
            mask = w
            r.append(w)
    return " ".join(r)


def pipe_silo(df, txt_col_name, syms, wordlist):
    lidx: int = None
    for i, sep in enumerate(syms):
        if sep != " ":
            pre_col_name = f"re{str(lidx)}" if lidx is not None else txt_col_name
            df = df.with_column(pl.col(pre_col_name).str.split(sep).alias(f"_re{str(i)}"))
            df = df.with_column(pl.col(f"_re{str(i)}").arr.join(" ").alias(f"re{str(i)}"))
            df = df.drop([f"_re{str(i)}"])
            if pre_col_name != txt_col_name:
                df = df.drop([pre_col_name])
            lidx = i
        df = df.with_column(pl.col(f"re{str(lidx)}").apply(partial(std_replace, wordlist=set(wordlist))).alias("silo"))
    return df


def _silo(df, txt_col_name, wordlist=None):
    wordlist = [] if wordlist is None else wordlist
    df = df.with_column(pl.col(txt_col_name).apply(partial(std_replace, wordlist=set(wordlist))).alias("silo"))
    return df


def pipe_count(df, txt_col_name):
    df = df.select(pl.col(txt_col_name).str.split(" ").flatten().alias("wordix"))
    df = df.groupby("wordix").agg(pl.count())
    df = df.sort("count", reverse=True)
    df = df.filter(pl.col("wordix").str.lengths() > 0)
    return df


def pipe_cmp(_df, date_column="datetime", pivot_date="2022-12-11 22:40:41", window_size: int = 1):
    pivot_day = dp.parse(pivot_date) if isinstance(pivot_date, str) else pivot_date
    start_day = pivot_day.day - window_size
    end_day = pivot_day.day + window_size
    start_date = dp.parse(
        f"{pivot_day.year}/{pivot_day.month}/{start_day} {pivot_day.hour}:{pivot_day.minute}:{pivot_day.second}"
    )
    end_date = dp.parse(f"{pivot_day.year}/{pivot_day.month}/{end_day} {pivot_day.hour}:{pivot_day.minute}:{pivot_day.second}")
    _df = _df.with_columns(
        [
            pl.when(pl.col(date_column).is_between(start=start_date, end=end_date, include_bounds=True))
            .then("Yes")
            .otherwise("No")
            .alias("match")
        ]
    )
    _df = _df.filter(pl.col("match") == "Yes")
    return _df.drop(["match"])


__all__ = ["pipe_is_null", "pipe_cmp_date", "std_map", "pipe_std_parse", "_silo", "pipe_count"]
