#!/usr/bin/env python
"""
Submit MG5_aMC@NLO jobs to condor. All jobs run using the same input card.

The user must specify the range of job IDs to submit. The job ID also sets the
random number generator seed, thus one should avoid using the same job ID more
than once. The user can also specify the output directory
(or one will be auto-generated).

To pass arguments to the MG5_aMC@NLO program, use the '--args' flag.
e.g. if you ran locally with:
'--card mycard.txt --seed 10'
you should call this script with:
'--args --card mycard.txt --seed 10'

Note that --args must be specified after all other arguments!

There is also the option for a 'dry run' where all the files & directories are
set up, but the job is not submitted.

Note that this submits the jobs not one-by-one but as a DAG, to allow easier
monitoring of job status.
"""


from time import strftime
from subprocess import check_call
import argparse
import sys
import os
import getpass
import logging
import re
from run_mg5 import MG5ArgParser
import htcondenser as ht


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

# Set the local MG5 install directory here.
MG5_DIR = '/users/%s/MG5_aMC/MG5_aMC_v2_3_3' % (os.environ['LOGNAME'])

# Set directory for STDOUT/STDERR/LOG from jobs
LOG_DIR = '/storage/%s/NMSSMPheno/MG5_aMC/' % os.environ['LOGNAME']


def submit_mc_jobs_htcondor(in_args=sys.argv[1:], mg5_dir=MG5_DIR, log_dir=LOG_DIR):
    """
    Main function. Sets up all the relevant directories, makes condor
    job and DAG files, then submits them if necessary.
    """

    # Handle user options. The user can pass all the same options as they
    # would if running the program locally. However, for certain options,
    # we interecpt the argument(s) and modify if necessary.
    parser = argparse.ArgumentParser(description=__doc__)

    # Options for the job submitter
    parser.add_argument("jobIdRange",
                        help="Specify job ID range to run over. The ID is used"
                        " as the random number generator seed, so manual "
                        "control is needed to avoid making the same files. "
                        "Must be of the form: startID, endID. ",
                        nargs=2, type=int)  # no metavar, bug with positionals
    parser.add_argument("--oDir",
                        help="Directory for output HepMC files. "
                        "If no directory is specified, an automatic one will "
                        "be created at: "
                        "/hdfs/user/<username>/NMSSMPheno/MG5_aMC/<output>/<date>"
                        ", where <output> refers to the output directory as "
                        "specified in the card.",
                        default="")
    # All other program arguments to pass to program directly.
    parser.add_argument("--args",
                        help="All other program arguments. "
                        "You MUST specify this after all other options",
                        nargs=argparse.REMAINDER)
    # Some generic script options
    parser.add_argument("--dry",
                        help="Dry run, don't submit to queue.",
                        action='store_true')
    parser.add_argument("-v",
                        help="Display debug messages.",
                        action='store_true')
    args = parser.parse_args(args=in_args)

    mg5_parser = MG5ArgParser()
    mg5_args = mg5_parser.parse_args(args.args)

    log.info('>>> Creating jobs')

    if args.v:
        log.setLevel(logging.DEBUG)

    log.debug('program args: %s', args)
    log.debug('mg5 args: %s', mg5_args)

    # Do some checks
    if not os.path.isdir(mg5_dir):
        raise RuntimeError('mg5_dir does not correspond to an actual directory')

    check_args(args)

    # Get the input card from user's options & check it exists
    card = mg5_args.card
    if not card:
        raise RuntimeError('You did not specify an input card!')
    if not os.path.isfile(card):
        raise RuntimeError('Input card %s does not exist!' % card)
    args.card = card
    args.channel = get_value_from_card(args.card, 'output')

    # Auto generate output directory if necessary
    if args.oDir == "":
        args.oDir = generate_dir_soolin(args.channel)
        log.info('Auto setting output dir to %s', args.oDir)

    # Zip up MG5 installation
    version = re.findall(r'MG5_aMC_v.*', mg5_dir)[0]
    mg5_zip = '%s.tgz' % version
    create_mg5_zip(mg5_dir, mg5_zip)

    # Make DAG
    file_stem = '%s/mg5_%s' % (generate_subdir(args.channel), strftime("%H%M%S"))
    log_dir = os.path.join(log_dir, generate_subdir(args.channel), 'logs')
    mg5_dag = create_dag(dag_filename=file_stem + '.dag',
                         condor_filename='HTCondor/mg5.condor',
                         status_filename=file_stem + '.status',
                         zip_filename=mg5_zip,
                         log_dir=log_dir, args=args)

    # Submit DAG
    if args.dry:
        log.warning('Dry run - not submitting jobs or copying files.')
        mg5_dag.write()
    else:
        mg5_dag.submit()

    return 0


def check_args(args):
    """Check sanity of input args.

    Parameters
    ----------
    args : argparse.Namespace
        Description

    Raises
    ------
    RuntimeError
        If jobIdRange invalid
    """
    if args.jobIdRange[0] < 1:
        raise RuntimeError('The first jobIdRange argument must be >= 1.')

    if args.jobIdRange[1] < args.jobIdRange[0]:
        raise RuntimeError('The second jobIdRange argument must be >= the first.')


def create_mg5_zip(mg5_dir, mg5_zip):
    """Create gzip compressed archive of MG5_aMC installation.

    Parameters
    ----------
    mg5_dir : str
        Path to MG5_aMC
    mg5_zip : str
        Name of resultant zip file.
    """
    version = re.findall(r'MG5_aMC_v.*', mg5_dir)[0]
    log.info('Creating tar file of MG5 installation, please wait...')
    check_call(['tar', 'czf', mg5_zip, '-C', os.path.dirname(mg5_dir), version])


def create_dag(dag_filename, condor_filename, status_filename, zip_filename, log_dir, args):
    """Make a htcondenser.DAGMan for a set of jobs.

    Creates a DAG file, adding extra flags for the worker node script.
    This includes setting the random number generator seed, and copying files
    to & from /hdfs. Also ensures a DAG status file will be written every 30s.

    dag_filename : str
        Name to be used for DAG job file.
    condor_filename : str
        Name of condor job file to be used for each job.
    status_filename : str
        Name to be used for DAG status file.
    zip_filename : str
        Name of MG5_aMC zip file.
    args : argparse.Namespace
        Contains info about output directory, job IDs, number of events per job,
        and args to pass to the executable.
    """
    # to parse the MG5 specific parts
    mg5_parser = MG5ArgParser()
    mg5_args = mg5_parser.parse_args(args.args)

    mg5_dag = ht.DAGMan(filename=dag_filename, status_file=status_filename)

    mg5_jobset = ht.JobSet(exe='run_mg5.py', copy_exe=True,
                           setup_script='HTCondor/setupMG.sh',
                           filename=condor_filename,
                           out_dir=log_dir, err_dir=log_dir, log_dir=log_dir,
                           memory="100MB", disk="2GB", share_exe_setup=True,
                           common_input_files=[mg5_args.card, zip_filename],
                           hdfs_store=args.oDir)

    for job_ind in xrange(args.jobIdRange[0], args.jobIdRange[1] + 1):
        mg5_job = generate_mg5_job(args, mg5_args, zip_filename, job_ind)
        mg5_jobset.add_job(mg5_job)
        mg5_dag.add_job(mg5_job)

    return mg5_dag


def generate_mg5_job(args, mg5_args, zip_filename, job_index):
    """Make a htcondenser.Job for

    Parameters
    ----------
    args : argparse.Namespace
        All args
    mg5_args : argparse.Namespace
        Args for run_mg5.py
    zip_filename : str
        Name of MG5 zip
    job_index : int
        Job index, specifies random number generator seed.

    Returns
    -------
    htcondenser.Job
        Job that runs MG5_aMC on input card file.
    """
    mg5_args.iseed = job_index  # RNG seed using job index
    mg5_args.exe = '%s/bin/mg5_aMC' % (os.path.basename(zip_filename).split('.')[0])

    # Options for the run_mg5.py script
    job_opts = [mg5_args.card]
    for k, v in mg5_args.__dict__.items():
        if k and v:
            if k == 'card':
                continue
            if k in ['hepmc', 'pythia8_path']:
                v = os.path.abspath(v)
            job_opts.extend(['--' + str(k), str(v)])

    # make some replacements due to different destination variable name
    # screwing things up. Yuck!
    remap = {'--iseed': '--seed', '--pythia8_path': '--pythia8'}
    for k, v in remap.items():
        job_opts[job_opts.index(k)] = v

    log.debug('job_opts: %s', job_opts)

    # Get the name of all the ouput files we want to copy to HDFS afterwards.
    # The --newstem tells run_mg5 to rename the output files
    output_dir = os.path.join(args.channel, 'Events', 'run_01')
    name_stem = '%s_n%d_seed%d' % (args.channel, mg5_args.nevents, mg5_args.iseed)
    job_opts.extend(['--newstem', name_stem])
    output_files = [os.path.join(output_dir, name_stem + '.lhe.gz'),
                    os.path.join(output_dir, name_stem + '.hepmc.gz'),
                    os.path.join(output_dir, 'RunMaterial_' + name_stem + '.tar.gz'),
                    os.path.join(output_dir, 'summary_' + name_stem + '.txt')]

    mg5_job = ht.Job(name='%d_%s' % (job_index, args.channel), args=job_opts,
                     output_files=output_files, hdfs_mirror_dir=args.oDir)
    return mg5_job


def generate_subdir(channel):
    """Generate a subdirectory name using channel and date.
    Can be used for output and log files, so consistent between both.

    >>> generate_subdir('ggh_4tau', 8)
    ggh_4tau/05_Oct_15
    """
    return os.path.join('%s' % (channel), strftime("%d_%b_%y"))


def generate_dir_soolin(channel):
    """Generate a directory name on /hdfs using userId, channel, and date.

    >>> generate_dir_soolin('ggh_4tau', 8)
    /hdfs/user/<username>/NMSSMPheno/MG5_aMC/ggh_4tau/<date>
    """
    uid = getpass.getuser()
    return "/hdfs/user/%s/NMSSMPheno/MG5_aMC/%s" % (uid, generate_subdir(channel))


def get_value_from_card(card, field):
    """Get value of field from card.

    Parameters
    ----------
    card : str
        Filename
    field : str
        Field name

    Returns
    -------
    str
        Value for the specified field.

    Raises
    ------
    KeyError
        If no field exists with the specified name.
    """
    with open(card) as f:
        for line in f:
            if field in line.strip():
                return line.strip().split()[-1]
        raise KeyError('Cannot find field with name %s' % field)


if __name__ == "__main__":
    sys.exit(submit_mc_jobs_htcondor())
