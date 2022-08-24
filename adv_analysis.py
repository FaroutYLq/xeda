import numpy as np
import matplotlib.pyplot as plt

used_tb_dict = {"project2": 20.59, "dali": 242.52}
limit_tb_dict = {"project2": 31.98, "dali": 266.00}
used_files_dict = {"project2": 4080907, "dali": 35813229}
limit_files_dict = {"project2": 5258000, "dali": 41357600}
alarm_tb_dict = {"project2": 1, "dali": 2}
alarm_files_dict = {"project2": 100000, "dali": 200000}


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
