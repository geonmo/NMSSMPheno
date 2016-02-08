#!/usr/bin/env python
"""
Script to submit a batch of Delphes jobs on HTCondor
"""


import argparse
import sys
sys.path.append('../Common')
import common
import os
import logging
from time import strftime
from subprocess import check_call
import htcondenser as ht


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


# Set the local Delphes installation directory here
DELPHES_DIR = '/users/%s/delphes' % os.environ['LOGNAME']

# Set directory for STDOUT/STDERR/LOG from jobs
LOG_DIR = '/storage/%s/NMSSMPheno/delphes' % os.environ['LOGNAME']


def submit_delphes_jobs_htcondor(in_args=sys.argv[1:], delphes_dir=DELPHES_DIR, log_dir=LOG_DIR):
    """Main function to submit jobs.

    Parameters
    ----------
    in_args : dict
        Arguments to parse
    delphes_dir : str, optional
        Directory with location of DelphesLHEF installation
    log_dir : str, optional
        Directory for STDOUT/STDERR/HTCondor logs.

    Raises
    ------
    OSError
        Description
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--card',
                        required=True,
                        help='Delphes card file. ASSUMES IT IS IN input_cards/')
    parser.add_argument('--iDir',
                        required=True,
                        help='Input directory of hepmc/lhe files to process')
    parser.add_argument('--type',
                        required=True,
                        choices=['hepmc', 'lhe'],
                        help='Filetype to process')
    parser.add_argument('--oDir',
                        help='Output directory for ROOT files. If one is not '
                        'specified, one will be created automatically at '
                        '<iDir>/../<card>_<type>')
    # Some generic script options
    parser.add_argument("--dry",
                        help="Dry run, don't submit to queue.",
                        action='store_true')
    parser.add_argument("-v",
                        help="Display debug messages.",
                        action='store_true')
    args = parser.parse_args(args=in_args)

    log.info('>>> Creating jobs')

    if args.v:
        log.setLevel(logging.DEBUG)

    log.debug('program args: %s' % args)

    # Avoid issues with os.path.dirname as we want parent directory, not itself
    if args.iDir.endswith('/'):
        args.iDir = args.iDir.rstrip('/')
    if delphes_dir.endswith('/'):
        delphes_dir = delphes_dir.rstrip('/')

    # Do some checks
    check_args(args, delphes_dir)

    # Auto-generate output dir if not specified
    if not args.oDir:
        args.oDir = generate_output_dir(args.iDir, os.path.basename(args.card), args.type)

    # Zip up Delphes installation
    delphes_zip = 'delphes.tgz'
    create_delphes_zip(delphes_dir, delphes_zip)

    # Write DAG file
    log_dir = os.path.join(log_dir, generate_subdir(args.card))
    file_stem = os.path.basename(os.path.splitext(args.card)[0]) + '_' + strftime("%H%M%S")
    delphes_dag = create_dag(dag_filename=file_stem + '.dag',
                             status_filename=file_stem + '.status',
                             condor_filename='HTCondor/runDelphes.condor',
                             log_dir=log_dir,
                             delphes_zip=delphes_zip,
                             args=args)

    # Submit it
    if args.dry:
        log.warning('Dry run - not submitting jobs or copying files.')
        delphes_dag.write()
    else:
        delphes_dag.submit()
    return 0


def check_args(args, delphes_dir):
    """Check user arguments.

    Parameters
    ----------
    args : argparse.Namespace
        Description
    delphes_dir : str
        Description

    Raises
    ------
    OSError
        If delphes_dir is not a directory.
        If input directory not a directory.
        If Card cannot be found.
        If Card not in input_cards.
    """
    if not os.path.isdir(delphes_dir):
        raise OSError('DELPHES_DIR does not correspond to an actual directory')
    if not os.path.isdir(args.iDir):
        raise OSError('--iDir arg does not correspond to an actual directory')
    if not os.path.isfile(args.card):
        raise OSError('Cannot find input card')
    if os.path.dirname(args.card) != 'input_cards':
        raise OSError('Put your card in input_cards directory')


def create_delphes_zip(delphes_dir, zip_filename='delphes.tgz'):
    """Create gzip compressed archive of Delphes directory.

    Parameters
    ----------
    delphes_dir : str
        Filepath to Delphes directory
    zip_filename : str, optional
        Name of resultant zip file
    """
    log.info('Creating tar file of Delphes installation, please wait...')
    check_call(['tar', 'czf', zip_filename, '-C',
                os.path.dirname(delphes_dir), os.path.basename(delphes_dir)])


def create_dag(dag_filename, status_filename, condor_filename, log_dir, delphes_zip, args):
    """Create a htcondenser.DAGMan to run Delphes over a set of files.

    Parameters
    ----------
    dag_filename: str
        Name to be used for DAG job file.
    status_filename: str
        Name to be used for DAG status file.
    condor_filename: str
        Name of condor job file to be used for each job.
    log_dir : str
        Name of directory to be used for log files.
    delphes_zip : str
        Location of delphes zip file.
    args: argparse.Namespace
        Contains info about output directory, job IDs, number of events per job,
        and args to pass to the executable.

    Returns
    -------
    htcondenser.DAGMan
        DAGMan for all delphes jobs.

    Raises
    ------
    OSError
        If no files in input directory of correct type (lhe, hepmc, gzipped)

    """
    # Collate list of input files
    def accept_file(filename):
        fl = os.path.basename(filename).lower()
        extensions = ['.lhe', '.hepmc', '.gz', '.tar.gz', '.tgz']
        return any([os.path.isfile(filename) and fl.endswith(ext) for ext in extensions])

    log.debug(os.listdir(args.iDir))
    abs_idir = os.path.abspath(args.iDir)
    input_files = [os.path.join(abs_idir, f) for f in os.listdir(abs_idir)
                   if accept_file(os.path.join(abs_idir, f))]
    if not input_files:
        raise OSError('No acceptable input file in %s' % args.iDir)

    # Setup DAGMan and JobSet objects
    # ------------------------------------------------------------------------
    log.info("DAG file: %s" % dag_filename)
    delphes_dag = ht.DAGMan(filename=dag_filename, status_file=status_filename)

    delphes_jobset = ht.JobSet(exe='HTCondor/runDelphes.py', copy_exe=True,
                               setup_script='HTCondor/setupDelphes.sh',
                               filename='HTCondor/delphes.condor',
                               out_dir=log_dir, err_dir=log_dir, log_dir=log_dir,
                               memory='100MB', disk='2GB',
                               share_exe_setup=True,
                               common_input_files=[delphes_zip, args.card],
                               transfer_hdfs_input=True,
                               hdfs_store=args.oDir)

    exe_dict = {'hepmc': './DelphesHepMC', 'lhe': './DelphesLHEF'}
    delphes_exe = exe_dict[args.type]

    # We assign each job to run over a certain number of input files.
    files_per_job = 2
    for ind, input_files in enumerate(common.grouper(input_files, files_per_job)):
        input_files = filter(None, input_files)

        job_args = ['--card', os.path.basename(args.card), '--exe', delphes_exe]

        # Add --process commands to job opts
        output_files = [os.path.join(args.oDir, stem(f)) + '.root' for f in input_files]
        for in_file, out_file in zip(input_files, output_files):
            job_args.extend(['--process', in_file, out_file])

        # Since we transfer across files on a one-by-one basis, we don't use
        # input_files or output_files for the input or outpt ROOT files.
        job = ht.Job(name='delphes%d' % ind, args=job_args)
        delphes_jobset.add_job(job)
        delphes_dag.add_job(job)

    return delphes_dag


def stem(filename):
    """Get rid of any .gz or .tar.gz"""
    return '.'.join(os.path.basename(filename).split('.')[0:1])


def generate_output_dir(input_dir, card, filetype):
    """Generate an output directory for Delphes ROOT files.

    >>> generate_output_dir('/hdfs/users/rob/hepmc', 'delphes_card_cms.tcl', 'hepmc')
    /hdfs/users/rob/delphes_card_cms_hepmc/

    Parameters
    ----------
    input_dir : str
        Name of directory with input files.
    card : str
        Name of card file.
    filetype : str
        Filetype of input (lhe, hepmc, etc)

    Returns
    -------
    str
        Output directory path
    """
    if input_dir.endswith('/'):
        input_dir = input_dir.rstrip('/')
    subdir = os.path.splitext(os.path.basename(card))[0] + "_" + filetype
    return os.path.join(os.path.dirname(input_dir), subdir)


def generate_subdir(card):
    """Generate subdirectory name using the card name and date.

    >>> generate_subdir('mycard.tcl')
    mycard/13_Jan_16
    """
    return os.path.join(os.path.splitext(os.path.basename(card))[0], strftime("%d_%b_%y"))


if __name__ == "__main__":
    sys.exit(submit_delphes_jobs_htcondor())
