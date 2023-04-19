import numpy as np
import matplotlib.pyplot as plt
from os import listdir
import datetime
from copy import deepcopy
from analysis import CATEGORIES


LIMIT_TB_DICT = {"project2": 31.98, "dali": 266.00}
LIMIT_FILES_DICT = {"project2": 5258000, "dali": 41357600}
ALARM_TB_DICT = {"project2": 1, "dali": 2}
ALARM_FILES_DICT = {"project2": 100000, "dali": 200000}
ALARM_SPECIFIC_TB_DICT = {"project2": 0.1, "dali": 0.2}
ALARM_SPECIFIC_FILES_DICT = {"project2": 50000, "dali": 100000}
ALARM_DELTA_TB_DICT = {"project2": 0.1, "dali": 0.1}
ALARM_DELTA_FILES_DICT = {"project2": 10000, "dali": 10000}

DB_DTYPE = [
    (("datetime of the scan", "time"), datetime.datetime),
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
        (df["total_n"] > ALARM_FILES_DICT[server])
        | (df["total_tb"] > ALARM_TB_DICT[server])
        # & (df["name"] != "xenonnt")
        # & (df["name"] != "xenon1t")
    )

    plt.fill_between(
        x=[0, ALARM_FILES_DICT[server]],
        y1=ALARM_TB_DICT[server] * 1024,
        color="k",
        alpha=0.1,
        label="safe zone",
    )

    for folder in df[mask]:
        plt.scatter(folder["total_n"], folder["total_tb"] * 1024, label=folder["name"])
    plt.legend(loc="lower left")
    plt.grid()
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
        (df["%s_n" % (dtype)] > ALARM_SPECIFIC_FILES_DICT[server])
        | (df["%s_tb" % (dtype)] > ALARM_SPECIFIC_TB_DICT[server])
        # & (df["name"] != "xenonnt")
        # & (df["name"] != "xenon1t")
    )

    plt.fill_between(
        x=[0, ALARM_SPECIFIC_FILES_DICT[server]],
        y1=ALARM_SPECIFIC_TB_DICT[server] * 1024,
        color="k",
        alpha=0.1,
        label="safe zone",
    )

    for folder in df[mask]:
        plt.scatter(
            folder["%s_n" % (dtype)],
            folder["%s_tb" % (dtype)] * 1024,
            label=folder["name"],
        )
    plt.legend(loc="lower left")
    plt.grid()
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
        time = datetime.datetime(
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


def track_user_history(db, user, server="dali", mode="size", show_last_n=30):
    assert mode == "size" or mode == "counts"

    if mode == "size":
        mode_str = "_tb"
        ylabel = "Size [TB]"
    elif mode == "counts":
        mode_str = "_n"
        ylabel = "Counts"

    user_docs = db[user]
    length = min(len(user_docs["total_tb"]), show_last_n)
    accumulated = np.zeros(length)
    times_argsort = user_docs["time"].argsort()
    result_times = np.sort(user_docs["time"])[-length:]

    plt.figure(dpi=200)
    plt.fill_between(
        x=result_times,
        y1=np.zeros(length),
        y2=user_docs["total" + mode_str][times_argsort][-length:],
        label="others",
        color="k",
        alpha=0.7,
    )
    for cat in CATEGORIES:
        plt.fill_between(
            x=result_times,
            y1=accumulated,
            y2=(accumulated + user_docs[cat + mode_str][times_argsort][-length:]),
            label=cat,
        )
        accumulated += user_docs[cat + mode_str][times_argsort][-length:]

    plt.legend(loc="lower left")
    plt.title("%s@%s" % (user, server))
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.grid()

    plt.show()


def track_server_history(db, server="dali", mode="size", show_last_n=30):
    assert mode == "size" or mode == "counts"

    if mode == "size":
        mode_str = "_tb"
        ylabel = "Size [TB]"
    elif mode == "counts":
        mode_str = "_n"
        ylabel = "Counts"

    # Assumed xenonnt will always be there
    times = db["xenonnt"]["time"]
    times_argsort = times.argsort()

    length = len(times)
    times = times[-length:]
    accumulated = np.zeros(length)
    server_doc = np.zeros_like(db["xenonnt"])
    server_doc["time"] = times

    users = list(db.keys())
    for user in users:
        user_doc = db[user]
        user_argsort = user_doc["time"].argsort()
        user_times = user_doc["time"][user_argsort]
        user_total = user_doc["total" + mode_str][user_argsort]
        # This user has been long existing
        if len(user_times) == len(times):
            server_doc["total" + mode_str][times_argsort] += user_total
            for cat in CATEGORIES:
                user_cat_total = user_doc[cat + mode_str][user_argsort]
                server_doc[cat + mode_str][times_argsort] += user_doc[cat + mode_str][
                    user_argsort
                ]

        else:
            for t in times:
                if t in user_times:
                    i_time_server = np.where(server_doc["time"] == t)[0]
                    i_time_user = np.where(user_doc[user_argsort]["time"] == t)[0]
                    server_doc["total" + mode_str][i_time_server] += user_total[
                        i_time_user
                    ]
                    for cat in CATEGORIES:
                        user_cat_total = user_doc[cat + mode_str][i_time_user]
                        server_doc[cat + mode_str][i_time_server] += user_cat_total

    result_times = np.sort(server_doc["time"])[-length:]
    plt.figure(dpi=200)
    plt.fill_between(
        x=result_times,
        y1=server_doc["total" + mode_str][times_argsort][-length:],
        label="others",
        color="k",
        alpha=0.7,
    )

    plt.grid()
    plt.axhline(LIMIT_TB_DICT[server], color="b")

    for cat in CATEGORIES:
        plt.fill_between(
            x=result_times,
            y1=accumulated[-length:],
            y2=accumulated + server_doc[cat + mode_str][times_argsort][-length:],
            label=cat,
        )
        accumulated += server_doc[cat + mode_str][times_argsort][-length:]

    today = datetime.date.today()
    starting_from = today - datetime.timedelta(days=show_last_n)
    plt.xlim(left = starting_from, right = today)

    plt.legend(loc="lower left")
    plt.title(server)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.show()


def compare_to_last_time(db, server="dali"):
    db = deepcopy(db)

    # Assumed xenonnt will always be there
    times = db["xenonnt"]["time"][-2:]
    times.sort()
    times = times[-2:]
    users = list(db.keys())

    d_size_gb = []
    d_n = []
    names = []

    for user in users:
        user_doc = db[user]
        user_argsort = user_doc["time"].argsort()
        user_times = user_doc["time"]
        user_times.sort()

        # if this user only shows up once
        if len(user_times) == 1:
            # this user just shows up
            if user_times[0] == times[-1]:
                d_size_gb.append(db[user]["total_tb"][user_argsort][0])
                d_n.append(db[user]["total_n"][user_argsort][0])
                names.append(user)
            # this user only showed up last time
            elif user_times[0] == times[-2]:
                d_size_gb.append(-db[user]["total_tb"][user_argsort][0])
                d_n.append(-db[user]["total_n"][user_argsort][0])
                names.append(user)
        # if this user at least shows up twice
        else:
            # the name shows up in both this time and last
            if user_times[-1] == times[-1] and user_times[-2] == times[-2]:
                d_size_gb.append(
                    db[user]["total_tb"][user_argsort][-1]
                    - db[user]["total_tb"][user_argsort][-2]
                )
                d_n.append(
                    db[user]["total_n"][user_argsort][-1]
                    - db[user]["total_n"][user_argsort][-2]
                )
                names.append(user)
            # missing this time
            elif user_times[-1] != times[-1] and user_times[-2] == times[-2]:
                d_size_gb.append(-db[user]["total_tb"][user_argsort][-2])
                d_n.append(-db[user]["total_n"][user_argsort][-2])
                names.append(user)
            # misisng last time
            elif user_times[-1] == times[-1] and user_times[-2] != times[-2]:
                d_size_gb.append(db[user]["total_tb"][user_argsort][-1])
                d_n.append(db[user]["total_n"][user_argsort][-1])
                names.append(user)
            # missing for both times
            else:
                pass

    d_size_gb = np.array(d_size_gb)
    d_size_gb = d_size_gb * 1024
    d_n = np.array(d_n)
    names = np.array(names)
    mask_alarm = (d_n > ALARM_DELTA_FILES_DICT[server]) | (
        d_size_gb > ALARM_DELTA_TB_DICT[server] * 1024
    )

    plt.figure(dpi=200)
    plt.scatter(d_n, d_size_gb, s=1)
    for n in names[mask_alarm]:
        plt.scatter(d_n[names == n], d_size_gb[names == n], label=n)
    plt.fill_between(
        [np.min(d_n), ALARM_DELTA_FILES_DICT[server]],
        y1=np.min(d_size_gb),
        y2=1024 * ALARM_DELTA_TB_DICT[server],
        color="k",
        alpha=0.1,
        label="safe zone",
    )
    plt.legend(loc="lower left")
    plt.xlabel("Increased counts")
    plt.ylabel("Increased size [GB]")
    plt.title(server + " " + str(times[-2])[:10] + " VS " + str(times[-1])[:10])
    plt.grid()

    plt.show()
