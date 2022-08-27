import numpy as np
import matplotlib.pyplot as plt
from os import listdir
from datetime import datetime
from analysis import CATEGORIES

used_tb_dict = {"project2": 20.59, "dali": 242.52}
limit_tb_dict = {"project2": 31.98, "dali": 266.00}
used_files_dict = {"project2": 4080907, "dali": 35813229}
limit_files_dict = {"project2": 5258000, "dali": 41357600}
alarm_tb_dict = {"project2": 1, "dali": 2}
alarm_files_dict = {"project2": 100000, "dali": 200000}

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


def scatter_usage(df, server="dali"):
    plt.figure(dpi=200)
    plt.scatter(df["total_n"], df["total_tb"] * 1024, s=1)
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("File counts")
    plt.ylabel("Size [GB]")
    plt.title(
        "%s: %s(%s)/%sTB; \n %s(%s)/%s files"
        % (
            server,
            np.round(df["total_tb"].sum(), decimals=2),
            used_tb_dict[server],
            limit_tb_dict[server],
            df["total_n"].sum(),
            used_files_dict[server],
            limit_files_dict[server],
        )
    )

    mask = (
        (
            (df["total_n"] > alarm_files_dict[server])
            | (df["total_tb"] > alarm_tb_dict[server])
        )
        & (df["name"] != "xenonnt")
        & (df["name"] != "xenon1t")
    )
    for folder in df[mask]:
        plt.scatter(folder["total_n"], folder["total_tb"] * 1024, label=folder["name"])
    plt.legend()

    plt.show()


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
