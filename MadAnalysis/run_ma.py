#!/usr/bin/env python
"""
Run MadAnalysis locally.

Takes care of untarring input files, etc. Requires an input JSON file specifying
sample name and location.
"""


import os
import sys
import argparse
import json
import logging
from subprocess import check_call


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


class MadAnalysisArgParser(argparse.ArgumentParser):
    """
    Class to handle parsing of options. This allows it to be used in other
    scripts (e.g. for HTCondor/PBS batch scripts)
    """
    def __init__(self, *args, **kwargs):
        super(MadAnalysisArgParser, self).__init__(*args, **kwargs)
        self.add_arguments()

    def add_arguments(self):
        self.add_argument('samples',
                          help='JSON with sample names and locations')
        self.add_argument('--exe',
                          help='Location of MadAnalysis executable',
                          default="MadAnalysis5Job")
        self.add_argument('--dry',
                          action='store_true',
                          help="Only make card, don't run MadAnalysis")
        self.add_argument("-v",
                          action='store_true',
                          help="Display debug messages.")


def run_ma(in_args=sys.argv[1:]):
    """Main routine"""
    parser = MadAnalysisArgParser(description=__doc__)
    args = parser.parse_args(in_args)
    if args.v:
        log.setLevel(logging.DEBUG)
    log.debug(args)

    check_args(args)

    # Interpret samples JSON
    # create a filelist for each set of samples.
    sample_dict = json_to_dict(args.samples)
    log.debug('Sample dictionary: %s', sample_dict)

    filelists = generate_filelists(sample_dict, os.path.dirname(args.samples))

    # Run MadAnalysis
    if not args.dry:
        setup_run_ma(args, filelists)

    return 0


def check_args(args):
    """Check sanity of user's args.

    Parameters
    ----------
    args : argparse.Namespace
        User's args

    Raises
    ------
    OSError
        If JSON samples file doesn't exist or isn't a file.
        If MadAnalysis exe does not exist.
        If user didn't specify the MadAnalysis5job exe.
    """
    if not os.path.isfile(args.samples):
        raise OSError('JSON samples file does not exist')
    if not os.path.isfile(os.path.realpath(os.path.realpath(args.exe))):
        raise OSError('MadAnalysis exe does not exist')
    exe_name = 'MadAnalysis5job'
    if not os.path.basename(args.exe).startswith(exe_name):
        raise OSError('You need the %s exe' % exe_name)


def json_to_dict(json_file):
    """Create dictionary from JSON file.

    Parameters
    ----------
    json_file : str
        Filename for JSON file describing samples.

    Returns
    -------
    name : dict
        Dict corresponding to JSON file.
    """
    with open(json_file) as jfile:
        json_dict = json.load(jfile)
    return json_dict


def setup_run_ma(args, filelists):
    """Run MadAnalysis exe over filelist for each sample.

    Also runs the corresponding setup.sh script.

    Parameters
    ----------
    args : argparse.Namespace
        User arguments from CL
    filelists : list[str]
        List of files which contain sample files.

    Raises
    ------
    OSError
        If setup.sh script cannot be found.
    """
    exe_abs = os.path.realpath(os.path.realpath(args.exe))
    setup_script = os.path.join(os.path.dirname(exe_abs), 'setup.sh')
    if not os.path.isfile(setup_script):
        raise OSError('No setup script as %s' % setup_script)
    setup_cmd = 'source %s;' % setup_script
    # we make a temporary inner directory to run from since the output from
    # MadAnalysis will be put in $PWD/../Output/
    tmp_dir = 'run_dir'
    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
    os.chdir(tmp_dir)
    for flist in filelists:
        cmd = ' '.join([setup_cmd, exe_abs, flist])
        log.debug(cmd)
        check_call(cmd, shell=True)


def generate_filelists(sample_dict, out_dir):
    """Generate list of files suitable for use as input to MadAnalysis.
    Each file list will be named after the sample it represents.

    Parameters
    ----------
    sample_dict : dict
        Dict of info for channels. Key is the channel name, value is a
        dict of information about the channel.
        Channels starting with #, !, _ will be ignored.
    out_dir : str
        Output directory for filelists.

    Returns
    -------
    list[str]
        List of the absolute filepaths for the file lists.
    """
    return [create_filelist(channel, chan_dict, out_dir)
            for channel, chan_dict in sample_dict.iteritems()
            if channel[0] not in ['#', '!', '_']]


def create_filelist(channel, chan_dict, out_dir):
    """Create a filelist suitable for passing to MadAnalysis.

    Parameters
    ----------
    channel : str
        Name of the channel. Used for the filelist filename.
    chan_dict : dict
        Dictionary of info corresponding to each channel
    out_dir : str
        Output directory for filelist.

    Returns
    -------
    str
        Filename of filelist.
    """
    filename = os.path.join(out_dir, channel)

    def dir_file_iter(dirs, ext):
        for d in dirs:
            for f in os.listdir(d):
                if (ext and f.endswith(ext)) or not ext:
                    yield os.path.join(d, f)

    with open(filename, 'w') as flist:
        log.debug('Writing filelist %s', channel)
        n_files = chan_dict['num']
        for i, f in enumerate(dir_file_iter(chan_dict['dirs'], '.root')):
            if i >= n_files and n_files > 0:
                break
            flist.write('%s\n' % f)

    return os.path.realpath(filename)


if __name__ == "__main__":
    sys.exit(run_ma())
