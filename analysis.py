import pandas as pd
import numpy as np
from tqdm import tqdm
import gc
import re
from os.path import exists
from datetime import datetime
import os

CATEGORIES = [
    "rawdata",
    "records",
    "peaks",
    "root",
    "pickle",
    "jobs",
    "figures",
    "zips",
    "hdf",
]

ANALYSIS_DTYPE = np.dtype(
    [
        ("name", "<U64"),
        ("total_tb", float),
        ("total_n", int),
        ("rawdata_tb", float),
        ("records_tb", float),
        ("peaks_tb", float),
        ("root_tb", float),
        ("pickle_tb", float),
        ("jobs_tb", float),
        ("figures_tb", float),
        ("zips_tb", float),
        ("hdf_tb", float),
        ("others_tb", float),
        ("rawdata_n", int),
        ("records_n", int),
        ("peaks_n", int),
        ("root_n", int),
        ("pickle_n", int),
        ("jobs_n", int),
        ("figures_n", int),
        ("zips_n", int),
        ("hdf_n", int),
    ]
)


class DUAnalyzer:
    def __init__(
        self,
        scan_within,
        input_dir,
        output_dir,
        threshold,
        deep_scan,
        print_result=True,
    ):
        if scan_within[-1] != "/":
            scan_within += "/"

        self.scan_within = scan_within
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.print_result = print_result
        self.deep_scan = deep_scan
        self.threshold = threshold

    def analyze(self):
        # welcome message
        print("\nDoing analysis now, this might take ~20 mins...\n")
        print("\nLoading input txt file...\n")

        # some dupilication
        deep_scan = self.deep_scan
        scan_within = self.scan_within

        # load scan result
        sizes_kb, paths, self.total_size_kb = self.load_scan()
        print("Loaded input txt file.\n")
        print("Analyzing depths, parents and types...")

        # analyze depth, parents and types
        depths, parents, types = self.determine_attributes(paths, self.scan_within)
        parent_names, parent_sizes = self.parent_rank(sizes_kb, depths, parents)
        print("Analyzed depths, parents and types.")
        print("Started to writing results...\n")

        # remove output files if they already exist for today
        if exists(self.output_dir):
            print("%s existed already. Removing it..." % (self.output_dir))
            os.remove(self.output_dir)
            print("Removed")

        # deep scan related
        print("\nDeep scan started... it may take ~10 mins.\n")
        # prepare the single deepscan path case.
        if type(deep_scan) == str:
            deep_scan = [deep_scan]
        # perform deep scanning。 if deep_scan == None then do nothing
        if type(deep_scan) == list:
            for parent in deep_scan:
                print("\nDeep scanning %s\n" % (parent))
                with open(self.output_dir, "a") as f:
                    f.write("Deeper scan for %s \n" % (parent))
                ss = sizes_kb[parents == parent]
                ps = paths[parents == parent]
                sw = scan_within + parent + "/"
                ds, pts, ts = self.determine_attributes(ps, sw)
                pns, pss = self.parent_rank(ss[:-1], ds, pts)
                analysis = self.types_by_parent(
                    ss[:-1], ds, pts, pss, pns, ts, print_result=self.print_result
                )
                with open(self.output_dir, "a") as f:
                    f.write("--------------\n")

        # main scan
        print("\nMain scan started... it may take ~15 mins.\n")
        self.parent_analysis = self.types_by_parent(
            sizes_kb,
            depths,
            parents,
            parent_sizes,
            parent_names,
            types,
            self.threshold,
            print_result=self.print_result,
        )

        # goodbye message
        print("\nAnalysis done!\n")
        print("Report saved at %s" % (self.output_dir))

    def load_scan(self):
        """Load scan result from specified txt file.

        Returns:
            sizes_kb (1darray): directory/file size in unit of KB.
            paths (1darray): name of all scanned paths.
            total_size_kb (int): total size in unit of KB.
        """
        gc.collect()
        dali_scan = pd.read_csv(
            self.input_dir,
            sep="\t.",
            header=None,
            encoding="ISO-8859-1",
            index_col=False,
        )

        sizes_kb = np.array(dali_scan.values[:, 0])
        paths = np.array(dali_scan.values[:, 1])

        # remove abnormal fields like below
        sizes_kb = sizes_kb[paths != None]
        paths = paths[paths != None]

        # remove non-ascii
        mask_ascii = np.ones(len(paths), dtype="bool")
        for i in range(len(paths) - 1):
            if type(sizes_kb[i]) == int:
                mask_ascii[i] = paths[i].isascii()
            else:
                mask_ascii[i] = sizes_kb[i].isascii() and paths[i].isascii()

        sizes_kb = sizes_kb[mask_ascii]
        paths = paths[mask_ascii]

        # transform to int for sizes
        sizes_kb = np.array(sizes_kb, dtype=int)

        # remove last line which is total disk usage
        paths = paths[:-1]
        total_size_kb = sizes_kb[-1]
        sizes_kb = sizes_kb[:-1]

        return sizes_kb, paths, total_size_kb

    def determine_attributes(self, paths, scan_within):
        """Given the paths, find the depths, parents and types of each path

        Args:
            paths (1darray): name of all scanned paths.
            scan_within (str): a path in which you scan. eg. '/dali/lgrandi/'

        Returns:
            depths (1darray): the depth of each path in absolute directory.
            parents (1darray): the second deepest folder in this path.
            types (1darray): the type of this path. if a directory, it reads 'others' too.
        """
        gc.collect()
        if scan_within != self.scan_within:
            paths = paths[:-1]
        depths = np.zeros(len(paths))
        parents = []
        types = []
        scan_within_split = scan_within.split("/")
        depth = len(scan_within_split) - 1

        print("depth = %s" % (depth))
        print("eg paths: %s" % (paths[0]))
        print("eg splits: %s" % (paths[0].split("/")))
        print("eg splits after selection: %s" % (paths[0].split("/")[depth - 1 :]))

        for i, p in tqdm(enumerate(paths)):
            splits = p.split("/")
            if (
                splits[0] != "dali" and splits[0] != "project2" and splits[0] != "home"
            ):  # in case you scaned using relative path...
                splits = splits[depth - 2 :]
            else:
                splits = splits[depth - 1 :]
            depths[i] = len(splits)

            parents.append(splits[0])
            last = splits[-1]
            if bool(re.match("reader[0-9]_reader_[0-9]_*", last)):
                types.append("rawdata")
            elif (
                ("-raw_records-" in last)
                or ("-records-" in last)
                or ("-raw_records_he-" in last)
                or ("-raw_records_aqmon-" in last)
                or ("-raw_records_mv-" in last)
                or ("-raw_records_nv-" in last)
                or ("-records_mv-" in last)
                or ("-records_nv-" in last)
            ):
                types.append("records")
            elif (
                ("-peaklets-" in last)
                or ("-peaks-" in last)
                or ("-lone_hits-" in last)
                or ("-hitlets-" in last)
            ):
                types.append("peaks")
            elif ".root" in last:
                types.append("root")
            elif (
                ("pickles" in last)
                or (".pkl" in last)
                or (".npz" in last)
                or (".npy" in last)
                or (".csv" in last)
            ):
                types.append("pickle")
            elif (".hdf" in last) or (".h5" in last):
                types.append("hdf")
            elif (".txt" in last) or (".log" in last) or (".sh" in last):
                types.append("jobs")
            elif (".png" in last) or (".jpg" in last) or (".jpeg" in last):
                types.append("figures")
            elif (".zip" in last) or (".gz" in last):
                types.append("zips")
            else:
                types.append("others")

        parents = np.array(parents)
        types = np.array(types)
        return depths, parents, types

    def parent_rank(self, sizes, depths, parents):
        """rank the parent names and sizes based on their sizes, from largest to smallest.

        Args:
            sizes (1darray): size of a path
            depths (1darray): the depth of each path in absolute directory.
            parents (1darray): the second deepest folder in this path.

        Returns:
            _type_: _description_
        """
        parent_sizes = sizes[depths == 1]
        parent_names = parents[depths == 1]

        # rank from largest to smallest
        parent_names = parent_names[np.argsort(parent_sizes)[::-1]]
        parent_sizes = parent_sizes[np.argsort(parent_sizes)[::-1]]

        return parent_names, parent_sizes

    def types_by_parent(
        self,
        sizes_kb,
        depths,
        parents,
        parent_sizes,
        parent_names,
        types,
        threshold=0.05,
        print_result=True,
    ):
        # Memory heavy job, so release some memory first
        gc.collect()

        # Init analysis dataframe
        analysis_dtype = ANALYSIS_DTYPE
        parent_analysis = np.zeros(len(parent_sizes), dtype=analysis_dtype)

        for i in tqdm(range(len(parent_sizes))):
            total = parent_sizes[i] / 1024**3  # in unit of TB
            others = total

            parent_analysis[i]["name"] = parent_names[i]
            parent_analysis[i]["total_tb"] = total
            parent_analysis[i]["total_n"] = 0

            for cat in CATEGORIES:
                # count files
                count = np.sum((parents == parent_names[i]) & (types == cat))
                parent_analysis[i][cat + "_n"] = count
                parent_analysis[i]["total_n"] += count

                # compute disk usage in unit of TB
                exec(
                    '%s = np.sum(sizes_kb[(parents==parent_names[i])&(types=="%s")])/1024**3'
                    % (cat, cat)
                )
                parent_analysis[i][cat + "_tb"] = eval(cat)

                # subtract the size of the found type from total size to get others
                others -= eval(cat)

            parent_analysis[i]["others_tb"] = others

            # Write output files if folder has size above threshold
            if total > threshold:
                with open(self.output_dir, "a") as f:
                    f.write(
                        str(parent_names[i])
                        + ": "
                        + str(np.around(total, decimals=3))
                        + " TB; "
                        + "%s files.\n" % (parent_analysis[i]["total_n"])
                    )
                    for cat in CATEGORIES:
                        f.write(
                            "    "
                            + cat
                            + ": "
                            + str(np.around(eval(cat), decimals=3))
                            + " TB"
                            + "  "
                            + str(np.around(100 * eval(cat) / total, decimals=3))
                            + "%  "
                            + str(parent_analysis[i][cat + "_n"])
                            + "\n"
                        )
                    f.write(
                        "    "
                        + "others: "
                        + str(np.around(others, decimals=3))
                        + " TB"
                        + "  "
                        + str(np.around(100 * others / total, decimals=3))
                        + "%\n"
                    )
                    f.write("--------------\n")

                if print_result:
                    print(parent_names[i], np.around(total, decimals=3), "TB")
                    for cat in CATEGORIES:
                        print(
                            "    ",
                            cat + ": ",
                            np.around(eval(cat), decimals=3),
                            "TB",
                            "  ",
                            np.around(100 * eval(cat) / total, decimals=3),
                            "%  " + str(parent_analysis[i][cat + "_n"]),
                        )
                    print(
                        "    ",
                        "others : ",
                        np.around(others, decimals=3),
                        "TB",
                        "  ",
                        np.around(100 * others / total, decimals=3),
                        "%",
                    )
                    print("--------------\n")
            else:
                with open(self.output_dir, "a") as f:
                    f.write(
                        str(parent_names[i])
                        + ": "
                        + str(np.around(total, decimals=3))
                        + " TB; "
                        + "%s files.\n" % (parent_analysis[i]["total_n"])
                    )
                if print_result:
                    print(parent_names[i], np.around(total, decimals=3), "TB")
        with open(self.output_dir, "a") as f:
            f.write("--------------\n")
            f.write("\n")
        if print_result:
            print("--------------\n")

        # Overview: usage by each type
        total_scanned_tb = parent_analysis["others_tb"].sum()
        for cat in CATEGORIES:
            total_scanned_tb += parent_analysis[cat + "_tb"].sum()

        with open(self.output_dir, "a") as f:
            f.write("Summary:" + "\n")
            f.write(
                "Total storage scanned: %s TB \n"
                % (np.round(total_scanned_tb, decimals=3))
            )
            for cat in CATEGORIES:
                f.write(
                    "    "
                    + cat
                    + " : "
                    + str(np.around(parent_analysis[cat + "_tb"].sum(), decimals=3))
                    + "TB"
                    + "  "
                    + str(
                        np.around(
                            100 * parent_analysis[cat + "_tb"].sum() / total_scanned_tb,
                            decimals=3,
                        )
                    )
                    + "%  "
                    + str(parent_analysis[cat + "_n"].sum())
                    + "\n"
                )
            f.write(
                "    "
                + "others : "
                + str(np.around(parent_analysis["others_tb"].sum(), decimals=3))
                + "TB"
                + "  "
                + str(
                    np.around(
                        100 * parent_analysis["others_tb"].sum() / total_scanned_tb,
                        decimals=3,
                    )
                )
                + "%\n"
            )
            f.write("==============\n")
        np.save(self.output_dir.replace(".txt", ".npy"), parent_analysis)
        print(
            "Analysis npy file saved at %s" % (self.output_dir.replace(".txt", ".npy"))
        )

        if print_result:
            print(
                "Total storage scanned: %s TB"
                % (np.round(total_scanned_tb, decimals=3))
            )
            for cat in CATEGORIES:
                print(
                    "    ",
                    cat + " : ",
                    np.around(parent_analysis[cat + "_tb"].sum(), decimals=3),
                    "TB",
                    "  ",
                    np.around(
                        100 * parent_analysis[cat + "_tb"].sum() / total_scanned_tb,
                        decimals=3,
                    ),
                    "%  " + str(parent_analysis[cat + "_n"].sum()),
                )
            print(
                "    ",
                "others : ",
                np.around(parent_analysis["others_tb"].sum(), decimals=3),
                "TB",
                "  ",
                np.around(
                    100 * parent_analysis["others_tb"].sum() / total_scanned_tb,
                    decimals=3,
                ),
                "%",
            )

        return parent_analysis
