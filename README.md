# xeda
**XEnon Disk usage Analysis**

## Scope
Analyze disk usage to see how much each type of data contribute. It does the following
- Use `du` command to scan over the target directory (excluding `rucio`), and save the results in a txt file we call "input". 
- Analyze the "input" directories
  - Show an overview for the target top directory.
  - Check the data type and corresponding size for each user.
  - Do one more depth deeper scan in the specified folders.
- Output the report in another txt file called "output". 
- History tracking and significant usage alarm.

## Usage
### Simplest
The simplest way to do a scan is to use the `batch_job.py` script, which submits the scanning jobs for both `dali` and `project2`. However, please keep in mind that if you want to do it this way, please change the paths which starts with `/home/yuanlq/...` to your own directory.

Make sure you are in the suitable environment for example: `source activate strax`, and you have the correct directories. Then just simply run:
```
python batch_job.py
```

### Canonical

First please make sure you are in the developement environment.

For typical usage, please check the examples below:
If you just want to have a scan at `/dali/lgrandi/` and put the report at `/project2/lgrandi/yuanlq/shared/disk_usage/`:

```
python xeda.py -t 500GB -o "/project2/lgrandi/yuanlq/shared/disk_usage/" -d "[xenonnt, xenon1t]" -s "/dali/lgrandi/"
```

- `-t` means threshold in data size to show in report. Here we picked 500GB. Only users who used more than 500GB will be shown analysis in report.
- `-o` means output path.
- `-d` means deeper scan at these folders.
- `-s` means scan at this top directory. (target of scan)

A more detailed explanation can be found below
| Key          | Meaning       | Example    | Default   | Required |
| :-----------:|:-------------:| :---------:|:---------:| ---------:|
| `-s`, `--scan_within` | Scope of analysis: target top directory of analysis. | `"/dali/lgrandi/"` | `"/dali/lgrandi/"` | True |
| `-i`, `--input_dir` | Input the existing `du` scan result, as a txt file. Not necessary, and the program will do the scan if it's not specified and file name will be generated automatically based on scope and datetime. | `"/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_input_20220306.txt"` | None | False |
| `-o`, `--output_dir` | Path or file name to output the analysis report. If you input a path instead of file name, the file name will be generated automatically based on scope and datetime. | `"/project2/lgrandi/yuanlq/shared/disk_usage/"` | None | True |
| `-d`, `--deep_scan` | Deeper scan at these folders. | `"[xenonnt, xenon1t, yuanlq]"` | None | False |
| `-t`, `--threshold` | Threshold of minimum folder size to be analyzed. | `1TB` | `'None'` | False |

## Examples
Please find then at
- Input file: `/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_input_20220306.txt`
- Output file: `/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_input_20220306.txt`

## Classifcation
|Class  | Meaning  |
| :-----------:|:-------------:| 
| `rawdata` | raw data from DAQ taken by REDAX |
| `records` | records level strax data, including `raw_records` and `records` for TPC regular channels and high energy channels, MV, NV and also acquisition monitor |
| `peaks` | peaks level strax data, including `peaklets`, `lone_hits`, `peaks` |
| `root` | `.root` data, typically are 1T-era minitrees and GEANT4 output |
| `pickle` | Usually user-defined data, including `.pickles`, `.pkl`, `.npz`, `npy` and `.csv` |
| `hdf` | Usually user-defined heavy data, including `.hdf` and `.h5` |
| `jobs` | Job submission related, including `.txt`, `.log` and `.sh` |
| `figures` | Human-readable figures including `.png`, `.jpg`, `.jpeg` |
| `zips` | Zip files including `.zip` or `.gz` |
| `others` | Other data type not classifed into any of them. Usually dominated by user-defined strax data |

## Analysis
Below is an example of some in-depth analysis beyond the report above.
```
import adv_analysis

# Load the analysis data sheet. Please modify the path in your case
dali = np.load('/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_output_20220823.npy')

# Scatter in file number VS size space
adv_analysis.scatter_usage(df=dali, server='dali')

# Track server history
db = adv_analysis.make_db(scan_within="/dali/lgrandi/", directory="/project2/lgrandi/yuanlq/shared/disk_usage/")
adv_analysis.track_server_history(db, server='dali', mode='size', show_last_n=20)

# Track user history
adv_analysis.track_user_history(db, user='xenonnt', server='dali', mode='size', show_last_n=20)

# Alarm if some users used significantly more compared to last scan
adv_analysis.compare_to_last_time(db, server="dali")
```

## Tar scripts
`batch_job_tar.py` submits jobs running `tar_and_split.py`, which tars and com and split the target directory or file into
```
python batch_job_tar.py /dali/lgrandi/yuanlq/
```
will tar and split `/dali/lgrandi/yuanlq/` and put output files in `/dali/lgrandi/yuanlq_relocated/`.

## Copy Pipeline
Copying data from dali storage to midway2 storage. Just do this in a screen session:
```
python copy_deamon.py
```
