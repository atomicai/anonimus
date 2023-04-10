import logging
import os
import uuid
from pathlib import Path

import plotly
import polars as pl
import pyarrow.parquet as pq
import random_name
from flask import jsonify, request, send_file, session
from icecream import ic
from werkzeug.utils import secure_filename

from anonimus.processing import pipe as ppipe
from anonimus.tooling.io import get_data

logger = logging.getLogger(__name__)
cache_dir = Path(os.getcwd()) / ".cache"


def upload():
    response = {}
    logger.info("welcome to upload`")
    xf = request.files["file"]
    prefixname = random_name.generate_name()
    filename = secure_filename(prefixname + xf.filename)
    ic(f"{filename}")
    if "uid" not in session.keys():
        uid = uuid.uuid4()
        session["uid"] = str(uid)
    else:
        uid = session["uid"]

    if not (cache_dir / str(uid)).exists():
        (cache_dir / str(uid)).mkdir(parents=True, exist_ok=True)
    destination = cache_dir / str(uid) / filename
    fname, fpath = Path(filename), Path(destination)
    df, columns = None, None
    is_suffix_ok, is_file_corrupted = True, False
    if fpath.suffix not in (".xlsx", ".csv"):
        is_suffix_ok = False
    else:
        try:
            xf.save(str(destination))
            df = next(
                get_data(
                    data_dir=cache_dir / uid,
                    filename=fname.stem,
                    ext=fname.suffix,
                    engine="polars",
                )
            )
        except:
            is_file_corrupted = True
        else:
            is_file_corrupted = False

    if is_suffix_ok and not is_file_corrupted:  # is suffix_ok is also false
        columns = [str(_) for _ in list(df.columns)]
        arr = df.to_arrow()
        pq.write_table(arr, cache_dir / uid / f"{fname.stem}.parquet")
        session["filename"] = filename
        session["prefixname"] = prefixname
        response["filename"] = filename
        response["text_columns"] = columns
    elif is_suffix_ok:
        msg = "There are some technical issues processing file. Please make sure the file is not corrupted"
        response["error"] = msg
        ic(msg)
    else:
        msg = f"The file format {fname.suffix} is not yet supported. The supported file formats are \".csv\" and \".xlsx\""
        response["error"] = msg
        ic(msg)
    response["is_suffix_ok"] = is_suffix_ok
    response["is_file_corrupted"] = is_file_corrupted
    return response


def iload():
    """
    This route will scan the file and perform the "lazy" way to push it to the database

    sets the `email` | `text` columns
    """
    data = request.get_json()
    #
    uid = session["uid"]
    filename = Path(session["filename"])
    text_column = data.get("text", None)
    is_text_ok = False
    df = pl.scan_parquet(cache_dir / uid / f"{filename.stem}.parquet")
    if text_column is not None and text_column in df.columns:
        session["text"] = text_column
        is_text_ok = True

    session["email"] = data.get("email", None)

    if is_text_ok:
        return jsonify({"success": "In progress to the moon🚀"})
    else:
        return jsonify({"Error": "Back to earth ♁. Fix the column name(s) 🔨"})


def download(filename):
    ic(session["uid"])
    uid = str(session["uid"])
    # df = df.filter(pl.col(session["text"]).is_not_null() | pl.col(session["text"]) == "")
    destination = cache_dir / str(uid) / f"the-{session['prefixname']}.xlsx"
    return send_file(str(destination), as_attachment=True)


def view():
    ic(session["uid"])
    uid = str(session["uid"])
    filename = Path(session["filename"])
    Path(Path(filename).stem + ".xlsx")
    df = pl.read_parquet(cache_dir / uid / f"{filename.stem}.parquet")
    df = df.filter(~pl.col(session["text"]).is_null())
    tic = ppipe.pipe_count(df, session["text"])
    # df = ppipe.pipe_silo(df, session["text"], syms=[":"], wordlist=None)  # TODO: add names and surnames
    df = ppipe._silo(df, session["text"])  # column name <silo>
    df = df.drop(session["text"])
    df = (
        df.with_row_count()
        .with_columns([pl.col("row_nr").last().over("silo").alias("idx_per_unique")])
        .filter(pl.col("row_nr") == pl.col("idx_per_unique"))
    )
    df = df.rename({"silo": session["text"]})
    df = df.drop(["row_nr", "idx_per_unique"])
    toc = ppipe.pipe_count(df, session["text"])
    print("silo")
    # df = df.filter(pl.col(session["text"]).is_not_null() | pl.col(session["text"]) == "")
    destination = cache_dir / uid / f"the-{session['prefixname']}.xlsx"
    df.write_excel(destination)
    return jsonify(
        {
            "num_words_deleted": tic.shape[0] - toc.shape[0],
            "result_file_size": destination.stat().st_size // 1024,
            "maximum_file_size": 29,
            "filename": "the-" + session["prefixname"],
        }
    )


__all__ = ["upload", "iload", "download", "view"]
