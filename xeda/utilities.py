import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
from utilix import xent_collection
from glob import glob


COLL = xent_collection()
MAX_RUN_NUMBER = COLL.count_documents({})
SR0_RIGHT = 34731
SR0_LEFT  = 17918
SR1_LEFT  = 43039
SR1_RIGHT = MAX_RUN_NUMBER

PEAKS_DTYPES = ['peaklets', 'merged_s2s', 'lone_hits', 'hitlets_nv']


def load_rules(datestr, directory='/project2/lgrandi/yuanlq/shared/rucio_scan'):
    """
    Load Rucio scan results.
    Args:
        datestr(str): date of the scan, e.g. '20210101'
        directory(str): directory of the scan, Default: '/project2/lgrandi/yuanlq/shared/rucio_scan'
    Returns:
        rules_info(array): rules table
    """
    print('Loading Rucio scan results from %s, scanned on %s'%(directory, datestr))

    globbed = glob('%s/rucio_%s_all_rules*.npy'%(directory, datestr))
    n_files = len(globbed)

    for i in tqdm(range(n_files)):
        if i == 0:
            rules_info = np.load('%s/rucio_%s_all_rules0.npy'%(directory, datestr), 
                                 allow_pickle=True)
        else:
            new = np.load('%s/rucio_%s_all_rules%s.npy'%(directory, datestr, i), 
                          allow_pickle=True)
            rules_info = np.concatenate((rules_info, new))

    return rules_info

def get_modes(rules):
    """
    Get run modes for the corresponding rule.
    Args:
        rules(array): rules table
    Returns:
        modes(array): array of run modes
    """
    modes = []
    runs = rules['runid'].astype(int)

    print('Loading run modes from RunDB...')
    for r in tqdm(runs):
        mode = COLL.find_one({'number': int(r)})['mode']
        modes.append(mode)

    return np.array(modes)

def get_mode_by_run(runids=None):
    """
    Get run modes for the corresponding runID.
    Args:
        runids(array): array of runIDs, Default: None, i.e. all runIDs
    Returns:
        modes(array): array of run modes
    """
    if runids is None:
        runids = np.arange(MAX_RUN_NUMBER)

    modes = []
    print('Loading run modes from RunDB...')
    for r in tqdm(runids):
        mode = COLL.find_one({'number': int(r)})['mode']
        modes.append(mode)

    return np.array(modes)

def find_with_mode(rules, mode):
    """
    Find rules with a specific mode.
    Args:
        rules(array): rules table
        mode(str): run mode
    Returns:
        rules(array): rules table with only the selected mode
    """
    is_mode = np.zeros(len(rules), bool)
    modes = get_mode_by_run()
    for i,r in enumerate(rules):
        if int(r['runid'])<MAX_RUN_NUMBER:
            if modes[int(r['runid'])] == mode:
                is_mode[i] = True

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

def plot_cum_sizes_tb(title, x_bins=100, x_range=(0, MAX_RUN_NUMBER), dpi=100, 
                      **rules):
    """
    Plot the cumulative size of rules as a function of runID.
    Args:
        title(str): plot title
        x_bins(int): number of bins for runID,  Default: 100
        x_range(tuple): range of runID, Default: (0, MAX_RUN_NUMBER)
        dpi(int): dpi of the plot, Default: 100
        **rules(**kwargs): dictionary of rules table
    """
    xs = np.linspace(x_range[0], x_range[1], x_bins)
    plt.figure(dpi=dpi)
    for i in range(len(rules)):
        name = list(rules.keys())[i]
        cum_sizes_tb = size_vs_runs(rules[name],
                                    runid_min=x_range[0], 
                                    runid_max=x_range[1], 
                                    nbins=x_bins)
        
        label = str(name)
        plt.plot(xs, cum_sizes_tb, label=label)
    plt.xlabel('RunID')
    plt.title(title)
    plt.ylabel('Size [TB]')
    plt.axvspan(SR0_LEFT, SR0_RIGHT, alpha=0.3, color='k', label='SR0')
    plt.axvspan(SR1_LEFT, SR1_RIGHT, alpha=0.3, color='r', label='SR1')
    plt.legend()

def check_by_mode(rules, title=None, graph=True, dpi=100, max_n_modes=8, wiki=True):
    """
    Compute the cumulative size and number counts of rules as a function of mode.
    Args:
        rules(array): rules table
        title(str): plot title
        graph(bool): whether to plot the result, Default: True
        dpi(int): dpi of the plot, Default: 100
        max_n_modes(int): maximum number of modes to plot, Default: 8
        wiki(bool): whether to print the result in wiki format, Default: True
    Returns:    
        unique_modes(array): unique modes
        sizes_tb(array): cumulative size of rules as a function of mode
    """
    modes = get_modes(rules)
    unique_modes = np.unique(modes)
    sizes_tb = []
    counts = []
    for mode in unique_modes:
        rules_of_mode = rules[modes==mode]
        sizes_tb.append(rules_of_mode['size_gb'].sum()/1024)
        counts.append(len(rules_of_mode))

    n_unique_modes = len(unique_modes)
    max_n_modes = min(n_unique_modes, max_n_modes)
    sizes_tb = np.array(sizes_tb)
    
    indecies = sizes_tb.argsort()
    sizes_tb = sizes_tb[indecies]
    unique_modes = unique_modes[indecies]

    if wiki:
        print('^ Mode ^ Size [TB] ^ Count ^')
        for i in range(len(n_unique_modes)):
            print('| {} | {:.2f} | {} |'.format(unique_modes[-i-1], 
                                                sizes_tb[-i-1], 
                                                counts[-i-1]))

    if graph:
        plt.figure(dpi=dpi)
        plt.bar(unique_modes[-max_n_modes:], sizes_tb[-max_n_modes:])
        plt.xticks(rotation=90)
        plt.ylabel('Size [TB]')
        if title is not None:
            plt.title(title)
        plt.show()
        
    return unique_modes, sizes_tb, counts
