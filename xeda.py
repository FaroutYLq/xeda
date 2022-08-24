from argparse import ArgumentParser
from tqdm import tqdm
from datetime import datetime
import os
from analyzer import DUAnalyzer

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
