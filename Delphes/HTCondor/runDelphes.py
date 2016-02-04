#!/usr/bin/env python
"""
This script is designed to setup and run Delphes on the worker node on HTCondor.
User should not run this script directly!

This is mainly responsible for:

- copying necessary inputs from hdfs
- running program
- copying various outputs to hdfs
"""


import os
import argparse
import sys
import shutil
from subprocess import check_call


def runDelphes(in_args=sys.argv[1:]):
    """Main routine"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--exe',
                        help="Delphes executable to run. "
                        "If not specified, it'll try and guess")
    parser.add_argument('--card', required=True,
                        help='Delphes card')
    parser.add_argument('--process', nargs=2, action='append',
                        help='File for Delphes to process, of the form: '
                        '<input file> <output file>')
    args = parser.parse_args(args=in_args)
    print args

    # Setting up Delphes, etc is handled in setupDelphes.sh
    os.chdir('delphes')

    # Run Delphes over files
    # -------------------------------------------------------------------------
    # To save disk space, we copy over a single file, process it,
    # then copy the result to its destination.
    for input_file, output_file in args.process:
        in_local = os.path.basename(input_file)

        copy_to_local(input_file, in_local)

        # unzip if necessary
        if need_unzip(in_local):
            print 'Unzipping', in_local
            check_call(['gunzip', in_local])
        in_local = '.'.join(in_local.split('.')[0:2])  # untarred filenmae

        # Run Delphes
        exe = args.exe if args.exe else determine_exe(os.path.splitext(in_local)[1])
        out_local = os.path.basename(output_file)
        check_call([exe, os.path.join('..', args.card), out_local, in_local])

        copy_from_local(out_local, output_file)
        os.remove(out_local)
        os.remove(in_local)

    return 0


def copy_to_local(source, dest):
    """Copy file from /hdfs, /storage, etc to local area."""
    if source.startswith('/hdfs'):
        source = source.replace('/hdfs', '')
        check_call(['hadoop', 'fs', '-copyToLocal', source, dest])
    else:
        if os.path.isfile(source):
            shutil.copy2(source, dest)
        elif os.path.isdir(source):
            shutil.copytree(source, dest)


def copy_from_local(source, dest):
    """Copy file from local area to e.g. /hdfs, /storage, etc"""
    if not os.path.isdir(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest))
    if dest.startswith('/hdfs'):
        dest = dest.replace('/hdfs', '')
        check_call(['hadoop', 'fs', '-copyFromLocal', '-f', source, dest])
    else:
        if os.path.isfile(source):
            shutil.copy2(source, dest)
        elif os.path.isdir(source):
            shutil.copytree(source, dest)


def need_unzip(filename):
    """Determine if file needs unzipping first.

    Returns
    -------
    bool
        True if unzipping required.
    """
    f = os.path.basename(filename)
    return any([f.endswith(ext) for ext in ['.gz']])


def determine_exe(extension):
    """Determine which Delphes executable is needed based on input filetype.

    Parameters
    ----------
    extension : str
        File extension.

    Raises
    ------
    RuntimeError
        If unrecognised filetype, or one Delphes cannot handle.
    """
    if extension in ['.hepmc']:
        return './DelphesHepMC'
    elif extension in ['.lhe', '.lhef']:
        return './DelphesLHEF'
    else:
        raise RuntimeError('Cannot determine which exe to use for %s' % extension)


if __name__ == "__main__":
    sys.exit(runDelphes())
