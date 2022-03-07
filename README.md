# xeda
XEnon Disk usage Analysis

## Scope
Analyze disk usage to see how much each type of data contribute. It does the following
- Use `du` command to scan over the target directory (excluding `rucio`), and save the results in a txt file we call "input". 
- Analyze the "input" directories
  - Show an overview for the target top directory.
  - Check the data type and corresponding size for each user.
  - Do one more depth deeper scan in the specified folders.
- Output the report in another txt file called "output". 

## Usage
First please make sure you are in the developement environment.

For typical usage, please check the examples below:
If you just want to have a scan at `/dali/lgrandi` and put the report at `/project2/lgrandi/yuanlq/shared/disk_usage/`:

```
python storage_scan.py -t 500GB -o "/project2/lgrandi/yuanlq/shared/disk_usage/" -d "[xenonnt, xenon1t]" -s "/dali/lgrandi"
```

- `-t` means threshold in data size to show in report. Here we picked 500GB. Only users who used more than 500GB will be shown analysis in report.
- `-o` means output path.
- `-d` means deeper scan at these folders.
- `-s` means scan at this top directory. (target of scan)

A more detailed explanation can be found below
| Key          | Meaning       | Example    | Default   | Required |
| :-----------:|:-------------:| :---------:|:---------:| ---------:|
| `-s`, `--scan_within` | Scope of analysis: target top directory of analysis. | `"/dali/lgrandi"` | `"/dali/lgrandi"` | True |
| `-i`, `--input_dir` | Input the existing `du` scan result, as a txt file. Not necessary, and the program will do the scan if it's not specified and file name will be generated automatically based on scope and datetime. | `"/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_input_20220306.txt"` | None | False |
| `-o`, `--output_dir` | Path or file name to output the analysis report. If you input a path instead of file name, the file name will be generated automatically based on scope and datetime. | `"/project2/lgrandi/yuanlq/shared/disk_usage/"` | None | True |
| `-d`, `--deep_scan` | Deeper scan at these folders. | `"[xenonnt, xenon1t, yuanlq]"` | `"[xenonnt, xenon1t]"` | False |
| `-t`, `--threshold` | Threshold of minimum folder size to be analyzed. | `1TB` | `'None'` | False |

## Examples
Please find then at
- Input file: `/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_input_20220306.txt`
- Output file: `/project2/lgrandi/yuanlq/shared/disk_usage/dali_lgrandi_input_20220306.txt`
