import os
from pathlib import Path
from datetime import datetime
from subprocess import run
import socket
import numpy as np
import adv_analysis

DALI_OUTPUT = np.load(
    "/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_output_20220914.npy"
)
PROJECT2_OUTPUT = np.load(
    "/project2/lgrandi/yuanlq/shared/disk_usage/project2_lgrandi_output_20220914.npy"
)


def test_scan_with_deep_scan():
    """test with deep scan."""
    dir_head = os.getcwd()
    output_dir = dir_head + "/tests/test_scans/"
    scan_dir = dir_head + "/tests/test_dir"

    # scan a small directory
    run(
        [
            "python",
            "xeda.py",
            "-t",
            "500GB",
            "-o",
            output_dir,
            "-d",
            "dir1",
            "-s",
            scan_dir,
        ]
    )

    # determine file names
    today = datetime.today().strftime("%Y%m%d")
    files_head = scan_dir[1:].replace("/", "_")
    input_filename = files_head + "_input_" + today + ".txt"
    output_filename = files_head + "_output_" + today + ".txt"
    npy_filename = files_head + "_output_" + today + ".npy"

    # find directories to filenames
    dir_to_input = output_dir + "/" + files_head + "_input_" + today + ".txt"
    dir_to_output = output_dir + "/" + files_head + "_output_" + today + ".txt"
    dir_to_npy = output_dir + "/" + files_head + "_output_" + today + ".npy"

    # build pathlib Path object
    input_file = Path(dir_to_input)
    output_file = Path(dir_to_output)
    npy_file = Path(dir_to_npy)

    # if pytest is run by user, check if we have the analysis files written
    if socket.gethostname()[:6] == "midway" or socket.gethostname()[:4] == "dali":
        assert input_file.exists(), "Input file doesn't exist at %s" % (input_file)
        assert output_file.exists(), "Output file doesn't exist at %s" % (output_file)
        assert npy_file.exists(), "Output npy file doesn't exist at %s" % (npy_file)

        run(["rm", dir_to_input])
        run(["rm", dir_to_output])
        run(["rm", dir_to_npy])

    # if pytest is run by github, do nothing
    else:
        pass


def test_without_deep_scan():
    """test without deep scan."""
    dir_head = os.getcwd()
    output_dir = dir_head + "/tests/test_scans/"
    scan_dir = dir_head + "/tests/test_dir"

    # scan a small directory
    run(
        [
            "python",
            "xeda.py",
            "-t",
            "60GB",
            "-o",
            output_dir,
            "-s",
            scan_dir,
        ]
    )

    # determine file names
    today = datetime.today().strftime("%Y%m%d")
    files_head = scan_dir[1:].replace("/", "_")
    input_filename = files_head + "_input_" + today + ".txt"
    output_filename = files_head + "_output_" + today + ".txt"
    npy_filename = files_head + "_output_" + today + ".npy"

    # find directories to filenames
    dir_to_input = output_dir + "/" + files_head + "_input_" + today + ".txt"
    dir_to_output = output_dir + "/" + files_head + "_output_" + today + ".txt"
    dir_to_npy = output_dir + "/" + files_head + "_output_" + today + ".npy"

    # build pathlib Path object
    input_file = Path(dir_to_input)
    output_file = Path(dir_to_output)
    npy_file = Path(dir_to_npy)

    # if pytest is run by user, check if we have the analysis files written
    if socket.gethostname()[:6] == "midway" or socket.gethostname()[:4] == "dali":
        assert input_file.exists(), "Input file doesn't exist at %s" % (input_file)
        assert output_file.exists(), "Output file doesn't exist at %s" % (output_file)
        assert npy_file.exists(), "Output npy file doesn't exist at %s" % (npy_file)

        run(["rm", dir_to_input])
        run(["rm", dir_to_output])
        run(["rm", dir_to_npy])

    # if pytest is run by github, do nothing
    else:
        pass


def test_scatter_usage():
    """test scatter usage in adv_analysis."""
    adv_analysis.scatter_usage(PROJECT2_OUTPUT, server="project2")
    adv_analysis.scatter_usage(DALI_OUTPUT, server="dali")


def test_track_user_history():
    """test track_user_history"""
    db_dali = adv_analysis.make_db(
        scan_within="/dali/lgrandi/",
        directory="/project2/lgrandi/yuanlq/shared/disk_usage/",
    )
    adv_analysis.track_server_history(
        db_dali, server="dali", mode="size", show_last_n=20
    )
    adv_analysis.track_server_history(
        db_dali, server="dali", mode="counts", show_last_n=20
    )
    adv_analysis.track_user_history(
        db_dali, user="xenonnt", server="dali", mode="size", show_last_n=20
    )
    adv_analysis.track_user_history(
        db_dali, user="xenonnt", server="dali", mode="counts", show_last_n=20
    )

    db_project2 = adv_analysis.make_db(
        scan_within="/dali/project2/",
        directory="/project2/lgrandi/yuanlq/shared/disk_usage/",
    )
    adv_analysis.track_server_history(
        db_project2, server="project2", mode="size", show_last_n=20
    )
    adv_analysis.track_server_history(
        db_project2, server="project2", mode="counts", show_last_n=20
    )
    adv_analysis.track_user_history(
        db_project2, user="xenonnt", server="project2", mode="counts", show_last_n=20
    )
    adv_analysis.track_user_history(
        db_project2, user="xenonnt", server="project2", mode="counts", show_last_n=20
    )


def test_track_user_history():
    """test track_user_history"""
    db_dali = adv_analysis.make_db(
        scan_within="/dali/lgrandi/",
        directory="/project2/lgrandi/yuanlq/shared/disk_usage/",
    )
    adv_analysis.compare_to_last_time(db_dali, server="dali")

    db_project2 = adv_analysis.make_db(
        scan_within="/dali/project2/",
        directory="/project2/lgrandi/yuanlq/shared/disk_usage/",
    )
    adv_analysis.compare_to_last_time(db_project2, server="project2")
