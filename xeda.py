from argparse import ArgumentParser
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import gc
import re
from tqdm import tqdm
from os.path import exists
from datetime import datetime
import os

# import os, shlex
# import utilix
# from utilix.batchq import *

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
        print("\nDoing analysis now, this might take ~20 mins...\n")
        print("\nLoading input txt file...\n")
        sizes_kb, paths, self.total_size_kb = self.load_scan()
        print("Loaded input txt file.\n")
        print("Analyzing depths, parents and types...")
        depths, parents, types = self.determine_attributes(paths, self.scan_within)
        parent_names, parent_sizes = self.parent_rank(sizes_kb, depths, parents)
        deep_scan = self.deep_scan
        scan_within = self.scan_within
        print("Analyzed depths, parents and types.")
        print("Started to writing results...\n")
        if exists(self.output_dir):
            print("%s existed already. Removing it..." % (self.output_dir))
            os.remove(self.output_dir)
            print("Removed")

        # deep scan related
        print("\nDeep scan started... it may take ~10 mins.\n")
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
        else:
            parent = deep_scan
            with open(self.output_dir, "a") as f:
                f.write("Deeper scan for %s \n" % (parent))
            ss = sizes_kb[parents == parent]
            ps = paths[parents == parent]
            sw = scan_within + parent + "/"
            ds, pts, ts = self.determine_attributes(ps, sw)
            pns, pss = self.parent_rank(ss[:-1], ds, pts)
            _ = self.types_by_parent(
                ss[:-1], ds, pts, pss, pns, ts, print_result=self.print_result
            )
            with open(self.output_dir, "a") as f:
                f.write("--------------\n")

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
        parent_sizes = sizes[depths == 1]
        parent_names = parents[depths == 1]
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
        gc.collect()
        analysis_dtype = np.dtype(
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
        parent_analysis = np.zeros(len(parent_sizes), dtype=analysis_dtype)

        for i in tqdm(range(len(parent_sizes))):
            total = parent_sizes[i] / 1024**3
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
                others -= eval(cat)
                parent_analysis[i][cat + "_tb"] = eval(cat)

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

        # Overview
        total_scanned_tb = parent_analysis["others_tb"].sum()
        for cat in CATEGORIES:
            total_scanned_tb += parent_analysis[cat + "_tb"].sum()

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
        return parent_analysis


def main():
    # Welcome messages
    print("\n")
    print("XEDA: XEnon Disk usage Analysis\n", "    Lanqing Yuan at UChicago, Mar 2022")
    print("\n")

    # Define arguments
    parser = ArgumentParser("Disk usage scan")
    parser.add_argument(
        "-t",
        "--threshold",
        choices=["None", "1TB", "500GB", "100GB"],
        default="None",
        help="Threshold for folders to show up in rough scan report. Do you want analysis on all folders all just folders with enough size?",
    )
    parser.add_argument(
        "-i",
        "--input_dir",
        help="Absolute directory to disk usage scan result text file, will re-scan if not specified.",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        required=True,
        help="Absolute directory to analysis report file, will assign name automatically if you do not specify the file name.",
    )
    parser.add_argument(
        "-d",
        "--deep_scan",
        default="[xenonnt, xenon1t]",
        help='A list of folders name to scan deeper and include in the report. eg. "[xenonnt, xenonnt, yuanlq]" OR yuanlq',
    )
    parser.add_argument(
        "-s",
        "--scan_within",
        default="/dali/lgrandi/",
        help="Absolute directory to scan. eg. /dali/lgrandi",
    )
    parse_args = parser.parse_args()
    args_dict = vars(parse_args)

    # Check on inputs
    assert (
        args_dict["scan_within"][0] == "/"
    ), "Please use absolute path for scan_within."
    assert args_dict["output_dir"][0] == "/", "Please use absolute path for output_dir."
    if args_dict["input_dir"] != None:
        args_dict["input_dir"][0] == "/", "Please use absolute path for input_dir."

    # Sanity check for users
    print("Below are the original input parameters:")
    print("Directory to scan: ", args_dict["scan_within"])
    print("Directory of disk usage scan result: ", args_dict["input_dir"])
    print("Directory of analysis report output: ", args_dict["output_dir"])
    print("Folders specified to be scanned one layer deeper: ", args_dict["deep_scan"])
    print("Folder size threshold to be shown in report: ", args_dict["threshold"])
    print("\n")

    # Output path check
    if args_dict["output_dir"][-4:] != ".txt":
        if args_dict["output_dir"][-1] != "/":
            args_dict["output_dir"] += "/"
        output_to = args_dict["output_dir"]
        args_dict["output_dir"] += make_filename(
            scan_within=args_dict["scan_within"], filetype="output"
        )
    else:
        output_to_index = len(args_dict["output_dir"]) - args_dict["output_dir"][
            ::-1
        ].index("/")
        output_to = args_dict["output_dir"][:output_to_index]

    # Assign input_dir if not specified
    if args_dict["input_dir"] == None:
        to_du = True
        args_dict["input_dir"] = output_to + make_filename(
            scan_within=args_dict["scan_within"], filetype="input"
        )
    else:
        to_du = False

    # Clean up formats
    if args_dict["scan_within"][-1] != "/":
        args_dict["scan_within"] += "/"
    if args_dict["deep_scan"] == None:
        args_dict["deep_scan"] = ["xenonnt", "xenon1t"]
    if args_dict["deep_scan"][-1] == "]" and args_dict["deep_scan"][0] == "[":
        # fomalize list
        args_dict["deep_scan"] = args_dict["deep_scan"].replace(" ", "")
        args_dict["deep_scan"] = list(args_dict["deep_scan"][1:-1].split(","))

    # After clean-up
    print("After format clean-up or automatic directory assignment:")
    print("Directory to scan: ", args_dict["scan_within"])
    print("Directory of disk usage scan result: ", args_dict["input_dir"])
    print("Directory of analysis report output: ", args_dict["output_dir"])
    print("Folders specified to be scanned one layer deeper: ", args_dict["deep_scan"])
    print("Folder size threshold to be shown in report: ", args_dict["threshold"])
    print("\n")

    # Disk usage scan if necessary
    if to_du:
        print(
            "Since no input txt file is specified, we are going to scan over %s. It can take hours, please be patient..."
            % (args_dict["scan_within"])
        )
        print("Running the following command: ")
        command = (
            "du -la --exclude="
            + args_dict["scan_within"]
            + "rucio "
            + args_dict["scan_within"]
            + ">"
            + args_dict["input_dir"]
        )
        print(command)
        print("The scan started at: " + str(datetime.now()))
        os.system(command)
        print(
            "Scan result txt file is just saved at %s now." % (args_dict["input_dir"])
        )
        print(
            "The scan finished at: "
            + str(datetime.now())
            + ". Thank you for your patience!\n"
        )

    # Analysis
    args_dict["threshold"] = determine_threshold(args_dict["threshold"])
    analyzer = DUAnalyzer(
        scan_within=args_dict["scan_within"],
        input_dir=args_dict["input_dir"],
        output_dir=args_dict["output_dir"],
        threshold=args_dict["threshold"],
        deep_scan=args_dict["deep_scan"],
    )

    analyzer.analyze()
    print("Finished")


def determine_threshold(threshold):
    if threshold == "1TB":
        return 1
    elif threshold == "500GB":
        return 0.5
    elif threshold == "100GB":
        return 0.1
    elif threshold == "None" or threshold == None:
        return 0


def make_filename(scan_within="/dali/lgrandi/", filetype="input"):
    # check on filetype
    assert filetype in ["input", "output"]

    # datetime suffix
    dtstr = datetime.today().strftime("%Y%m%d")

    # make head of the filename
    if scan_within[-1] == "/":
        head = scan_within[1:].replace("/", "_")
    else:
        head = (scan_within[1:] + "/").replace("/", "_")

    # make tail of the filename
    tail = filetype + "_" + dtstr + ".txt"

    filename = head + tail
    return filename


if __name__ == "__main__":
    main()
    exit(13)
