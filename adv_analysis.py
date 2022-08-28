import numpy as np
import matplotlib.pyplot as plt
from os import listdir
from datetime import datetime
from analysis import CATEGORIES


LIMIT_TB_DICT = {"project2": 31.98, "dali": 266.00}
LIMIT_FILES_DICT = {"project2": 5258000, "dali": 41357600}
ALARM_TB_DICT = {"project2": 1, "dali": 2}
ALARM_FILES_DICT = {"project2": 100000, "dali": 200000}
ALARM_SPECIFIC_TB_DICT = {"project2": 0.5, "dali": 1}
ALARM_SPECIFIC_FILES_DICT = {"project2": 50000, "dali": 100000}

DB_DTYPE = [
    (("datetime of the scan", "time"), datetime),
    (("total size in TB unit", "total_tb"), np.float32),
    (("total file counts", "total_n"), np.int32),
    (("rawdata size in TB unit", "rawdata_tb"), np.float32),
    (("rawdata_file counts", "rawdata_n"), np.int32),
    (("records size in TB unit", "records_tb"), np.float32),
    (("records file counts", "records_n"), np.int32),
    (("peaks size in TB unit", "peaks_tb"), np.float32),
    (("peaks file counts", "peaks_n"), np.int32),
    (("root size in TB unit", "root_tb"), np.float32),
    (("root file counts", "root_n"), np.int32),
    (("pickle size in TB unit", "pickle_tb"), np.float32),
    (("pickle file counts", "pickle_n"), np.int32),
    (("job files size in TB unit", "jobs_tb"), np.float32),
    (("job file counts", "jobs_n"), np.int32),
    (("figures size in TB unit", "figures_tb"), np.float32),
    (("figures file counts", "figures_n"), np.int32),
    (("zips size in TB unit", "zips_tb"), np.float32),
    (("zips file counts", "zips_n"), np.int32),
    (("hdf size in TB unit", "hdf_tb"), np.float32),
    (("hdf file counts", "hdf_n"), np.int32),
]


def scatter_total_usage(df, server="dali"):
    plt.figure(dpi=200)
    plt.scatter(df["total_n"], df["total_tb"] * 1024, s=1)
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("File counts")
    plt.ylabel("Size [GB]")
    plt.title(
        "%s total: %s/%sTB; \n %s/%s files"
        % (
            server,
            np.round(df["total_tb"].sum(), decimals=2),
            LIMIT_TB_DICT[server],
            df["total_n"].sum(),
            LIMIT_FILES_DICT[server],
        )
    )

    mask = (
        (
            (df["total_n"] > ALARM_FILES_DICT[server])
            | (df["total_tb"] > ALARM_TB_DICT[server])
        )
        & (df["name"] != "xenonnt")
        & (df["name"] != "xenon1t")
    )

    plt.fill_between(
        x=[0, ALARM_FILES_DICT[server]],
        y1=ALARM_TB_DICT[server] * 1024,
        color="k",
        alpha=0.1,
    )

    for folder in df[mask]:
        plt.scatter(folder["total_n"], folder["total_tb"] * 1024, label=folder["name"])
    plt.legend(loc="lower left")

    plt.show()


def scatter_specific_usage(df, server="dali", dtype="root"):
    assert (
        dtype in CATEGORIES
    ), "Please input a valid type among the following: \n%s" % (CATEGORIES)

    plt.figure(dpi=200)
    plt.scatter(df["%s_n" % (dtype)], df["%s_tb" % (dtype)] * 1024, s=1)
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("File counts")
    plt.ylabel("Size [GB]")
    plt.title(
        "%s %s: %sTB; %s files"
        % (
            server,
            dtype,
            np.round(df["%s_tb" % (dtype)].sum(), decimals=2),
            df["%s_n" % (dtype)].sum(),
        )
    )

    mask = (
        (
            (df["%s_n" % (dtype)] > ALARM_SPECIFIC_FILES_DICT[server])
            | (df["%s_tb" % (dtype)] > ALARM_SPECIFIC_TB_DICT[server])
        )
        & (df["name"] != "xenonnt")
        & (df["name"] != "xenon1t")
    )

    plt.fill_between(
        x=[0, ALARM_SPECIFIC_FILES_DICT[server]],
        y1=ALARM_SPECIFIC_TB_DICT[server] * 1024,
        color="k",
        alpha=0.1,
    )

    for folder in df[mask]:
        plt.scatter(
            folder["%s_n" % (dtype)],
            folder["%s_tb" % (dtype)] * 1024,
            label=folder["name"],
        )
    plt.legend(loc="lower left")

    plt.show()


def scatter_usage(df, server="dali"):
    scatter_total_usage(df, server)

    for cat in CATEGORIES:
        scatter_specific_usage(df, server, cat)


def find_file_list(
    scan_within="/dali/lgrandi/",
    directory="/project2/lgrandi/yuanlq/shared/disk_usage/",
):
    assert (
        scan_within[0] == "/" and scan_within[-1] == "/"
    ), "Please check format of scan_within. Good example: /dali/lgrandi/"
    head = scan_within[1:-1].replace("/", "_")
    head_len = len(head)

    all_files = listdir(directory)
    files_oi = []
    for file_i in all_files:
        if file_i[-4:] == ".npy" and file_i[:head_len] == head:
            files_oi.append(file_i)

    return files_oi


def clean_up_date(date):
    assert len(date) == 2, "This function only works for month and day!"
    if date[0] == "0":
        date = date[-1]
    return eval(date)


def write_doc(time, array):
    new_doc = np.zeros(1, dtype=DB_DTYPE)

    new_doc["time"][0] = time
    new_doc["total_tb"] = array["total_tb"]
    new_doc["total_n"] = array["total_n"]

    for cat in CATEGORIES:
        new_doc["%s_tb" % (cat)] = array["%s_tb" % (cat)]
        new_doc["%s_n" % (cat)] = array["%s_n" % (cat)]

    return new_doc


def make_db(
    scan_within="/dali/lgrandi/",
    directory="/project2/lgrandi/yuanlq/shared/disk_usage/",
):
    files_oi = find_file_list(scan_within, directory)
    db = {}

    for file_oi in files_oi:
        time = datetime(
            int(file_oi[-12:-8]),
            clean_up_date(file_oi[-8:-6]),
            clean_up_date(file_oi[-6:-4]),
        )
        array = np.load(directory + file_oi)
        names = array["name"]
        for name in names:
            new_doc = write_doc(time, array[names == name])
            if name in db:
                length = len(db[name])
                old_docs = db[name]
                db[name] = np.concatenate((old_docs, new_doc))
            else:
                db[name] = write_doc(time, array[names == name])

    return db


def track_user_history(db, user, server="dali", mode="size", show_last_n=20):
    assert mode == "size" or mode == "counts"

    user_docs = db[user]
    length = min(len(user_docs["total_tb"]), show_last_n)
    accumulated = np.zeros(length)

    if mode == "size":
        mode_str = "_tb"
    elif mode == "counts":
        mode_str = "_n"

    plt.figure(dpi=200)
    plt.fill_between(
        x=user_docs["time"][-length:],
        y1=np.zeros(length),
        y2=1024 * user_docs["total" + mode_str][-length:],
        label="others",
        color="k",
        alpha=0.7,
    )
    for cat in CATEGORIES:
        plt.fill_between(
            x=user_docs["time"][-length:],
            y1=1024 * accumulated,
            y2=1024 * (accumulated + user_docs[cat + mode_str][-length:]),
            label=cat,
        )
        accumulated += user_docs[cat + mode_str][-length:]

    plt.legend(loc="lower left")
    plt.title("%s@%s" % (user, server))
    plt.ylabel("Size [GB]")
    plt.xticks(rotation=45)

    plt.show()
