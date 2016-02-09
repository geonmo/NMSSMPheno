#!/bin/env python
"""
This script is designed to setup and run on the worker node on HTCondor.
User should not run this script directly!

This is mainly responsible for:

- setting up environment, programs
- copying necessary inputs from hdfs, renaming if necessary
- running program
- copying various outputs to hdfs, renaming if necessary
"""


import argparse
from subprocess import call
import sys
import shutil
import os
import tarfile
from glob import glob


def main(in_args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--copyToLocal", nargs=2, action='append',
                        help="Files to copy to local area on worker node "
                        "before running program. "
                        "Must be of the form <source> <destination>. "
                        "Repeat for each file you want to copy.")
    parser.add_argument("--copyFromLocal", nargs=2, action='append',
                        help="Files to copy from local area on worker node "
                        "after running program. "
                        "Must be of the form <source> <destination>. "
                        "Repeat for each file you want to copy.")
    parser.add_argument("--args", nargs=argparse.REMAINDER,
                        help="")
    args = parser.parse_args(args=in_args)
    print args

    # Make sandbox area to avoid names clashing, and stop auto transfer
    # back to submission node
    # -------------------------------------------------------------------------
    os.mkdir('scratch')
    os.environ['SCRATCH'] = os.path.realpath('scratch')
    os.chdir('scratch')

    # Copy files to worker node area from /users, /hdfs, /storage, etc.
    # -------------------------------------------------------------------------
    if args.copyToLocal:
        for (source, dest) in args.copyToLocal:
            print source, dest
            if source.startswith('/hdfs'):
                source = source.replace('/hdfs', '')
                call(['hadoop', 'fs', '-copyToLocal', source, dest])
            else:
                if os.path.isfile(source):
                    shutil.copy2(source, dest)
                elif os.path.isdir(source):
                    shutil.copytree(source, dest)
        print os.listdir(os.getcwd())

    # Setup MG5_aMC
    # -------------------------------------------------------------------------
    mg5_tar = glob('MG5_aMC*')
    if len(mg5_tar) > 1:
        raise RuntimeError('Too many files/dirs for MG5_aMC*')
    elif not mg5_tar:
        raise RuntimeError('Cannot find MG5 tar.')
    mg5_tar = mg5_tar[0]
    with tarfile.open(mg5_tar) as tar:
        tar.extractall()
    os.remove(mg5_tar)
    mg5_dir = glob('MG5_aMC*')[0]

    # Run the program
    # -------------------------------------------------------------------------
    mg5_args = args.args

    # overwrite the existing exe path
    mg5_args.extend(['--exe', os.path.join(mg5_dir, 'bin', 'mg5_aMC')])
    print mg5_args

    sys.path.insert(0, os.path.realpath('.'))
    import run_mg5
    run_args = run_mg5.run_mg5(mg5_args)
    print run_args

    # Copy files from worker node area to /hdfs or /storage
    # -------------------------------------------------------------------------
    if args.copyFromLocal:
        for (source, dest) in args.copyFromLocal:
            check_create_dir(os.path.dirname(dest))
            print source, dest
            if dest.startswith('/hdfs'):
                dest = dest.replace('/hdfs', '')
                call(['hadoop', 'fs', '-copyFromLocal', '-f', source, dest])
            else:
                if os.path.isfile(source):
                    shutil.copy2(source, dest)
                elif os.path.isdir(source):
                    shutil.copytree(source, dest)


def check_create_dir(directory, info=False):
    """Check dir exists, if not create"""
    if not os.path.isdir(directory):
        if os.path.isfile(directory):
            raise RuntimeError('%s already exists as a file' % directory)
        os.makedirs(directory)
        if info:
            print 'Making dir', directory


def get_value_from_card(card, field):
    """Get value of field from card.

    card: str
        Filename
    field: str
        Field name
    """
    with open(card) as f:
        for line in f:
            if field in line.strip():
                return line.strip().split()[-1]


if __name__ == "__main__":
    main()
