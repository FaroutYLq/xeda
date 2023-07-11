import numpy as np
from tqdm import tqdm
from utilix import xent_collection
MAX_RUN_NUMBER = xent_collection().count_documents({})


def find_with_mode(rules, mode):
    """
    Find rules with a specific mode.
    Args:
        rules(array): rules table
        mode(str): run mode
    Returns:
        rules(array): rules table with only the selected mode
    """
    runs = rules['runid'].astype(int)
    is_mode = np.zeros(len(rules), bool)
    for i,r in enumerate(rules):
        if int(r['runid'])<MAX_RUN_NUMBER:
            if runs[int(r['runid'])]['mode'] == mode:
                is_mode[i] = True
    print(np.sum(is_mode))
    return rules[is_mode]

def size_vs_runs(rules, runid_min=0, runid_max=MAX_RUN_NUMBER, nbins=100):
    """
    Compute the cumulative size of rules as a function of runID.
    Args:
        rules(array): rules table
        runid_min(int): minimum runID
        runid_max(int): maximum runID
        nbins(int): number of bins
    Returns:
        cum_sizes_tb(array): cumulative size of rules as a function of runID
    """
    runids = rules['runid'].astype(np.int32)
    bins_bound = np.linspace(runid_min, runid_max, nbins+1)
    sizes_tb = np.zeros(nbins)
    for b in range(nbins):
        selected_mask = runids>=bins_bound[b]
        selected_mask &= runids<=bins_bound[b+1]
        rules_selected = rules[selected_mask]
        if len(rules_selected):
            sizes_tb[b] = np.sum(rules_selected['size_gb']/1024)
    
    cum_sizes_tb = np.cumsum(sizes_tb)
    return cum_sizes_tb

def find_with_tags(rules, tags):
    """
    Find rules with a specific tag.
    Args:
        rules(array): rules table
        tags(list): list of tags in RunDB
    Returns:
        rules(array): rules table with only the selected tags
    """
    runs = rules['runid'].astype(int)
    tagged = np.zeros(len(rules), bool)
    for i,r in enumerate(rules):
        for t in tags:
            if runs[int(r['runid'])][t]:
                tagged[i] = True
    print(np.sum(tagged))
    return rules[tagged]

def filter_out_rad(rules):
    """
    Filter out rules from RAD commissioning runs.
    Args:
        rules(array): rules table
    Returns:
        rules(array): rules table without RAD commissioning runs
    """
    runs = rules['runid'].astype(int)
    is_rad = np.zeros(len(rules), bool)
    for i,r in enumerate(rules):
        if runs[int(r['runid'])]['RAD_commissioning']:
            is_rad[i] = True
    return rules[~is_rad]

def keep_unique_runs(rules):
    """
    Keep only one rule per runID.
    Args:
        rules(array): rules table
    Returns:
        rules(array): rules table with only one rule per runID
    """
    unique_runs = np.unique(rules['runid'])
    unique_run_rules = np.zeros(len(unique_runs), dtype=rules.dtype)
    for i,runid in enumerate(tqdm(unique_runs)):
        unique_run_rules[i] = rules[rules['runid']==runid][0]
    return rules

def in_runlist(rules, runlist):
    """
    Keep only rules from a list of runIDs.
    Args:
        rules(array): rules table
        runlist(array): list of runIDs
    Returns:
        rules(array): rules table with only the selected runIDs
    """
    runids = rules['runid'].astype(int)
    indices = np.where(np.isin(runids, runlist.astype(int)))[0]
    return rules[indices]