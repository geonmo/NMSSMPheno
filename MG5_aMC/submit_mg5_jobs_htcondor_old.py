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


import shutil
from time import strftime
from subprocess import call
import argparse
import sys
import os
import getpass
import logging
import re
from run_mg5 import MG5ArgParser


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

# Set the local MG5 install directory here.
MG5_DIR = '/users/%s/MG5_aMC/MG5_aMC_v2_3_3' % (os.environ['LOGNAME'])


def submit_mc_jobs_htcondor(in_args=sys.argv[1:], mg5_dir=MG5_DIR):
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
                        "/hdfs/user/<username>/NMSSMPheno/MG5_aMC/<energy>TeV/<output>/<date>"
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

    log.debug('program args: %s' % args)
    log.debug('mg5 args: %s' % mg5_args)

    # Do some checks
    # -------------------------------------------------------------------------
    if not os.path.isdir(mg5_dir):
        raise RuntimeError('MG5_DIR does not correspond to an actual directory')

    if args.jobIdRange[0] < 1:
        raise RuntimeError('The first jobIdRange argument must be >= 1.')

    if args.jobIdRange[1] < args.jobIdRange[0]:
        raise RuntimeError('The second jobIdRange argument must be >= the first.')

    # Get the input card from user's options & check it exists
    card = mg5_args.card
    if not card:
        raise RuntimeError('You did not specify an input card!')
    if not os.path.isfile(card):
        raise RuntimeError('Input card %s does not exist!' % card)
    if os.path.dirname(card) != 'input_cards':
        raise RuntimeError('Put your card in input_cards directory')
    args.card = card
    args.channel = get_value_from_card(args.card, 'output')

    # Get CoM energy
    # -------------------------------------------------------------------------
    args.energy = int(get_value_from_card(args.card, 'ebeam1')) * 2 / 1000

    # Auto generate output directory if necessary
    # -------------------------------------------------------------------------
    if args.oDir == "":
        args.oDir = generate_dir_soolin(args.channel, args.energy)

    check_create_dir(args.oDir)

    # Dicts to hold thing to be copied before/after the job runs
    copy_to_local = {}
    copy_from_local = {}

    # Make the program zip and put it in hdfs
    # -------------------------------------------------------------------------
    # don't want any trailing "/"
    if mg5_dir.endswith("/"):
        mg5_dir = mg5_dir.rstrip('/')
    version = re.findall(r'MG5_aMC_v.*', mg5_dir)[0]
    log.info('Creating tar file of MG5 installation, please wait...')
    call(['tar', 'czf', '%s.tgz' % version, '-C', os.path.dirname(mg5_dir), version])
    # copy to hdfs
    zip_dir = '/hdfs/user/%s/NMSSMPheno/zips/' % (os.environ['LOGNAME'])
    check_create_dir(zip_dir)
    zip_filename = '%s.tgz' % version
    zip_path = os.path.join(zip_dir, zip_filename)
    call(['hadoop', 'fs', '-copyFromLocal', '-f', zip_filename, zip_dir.replace('/hdfs', '')])
    os.remove(zip_filename)
    copy_to_local[zip_path] = 'MG5_aMC.tgz'

    # Copy across input cards to hdfs to sandbox them
    # -------------------------------------------------------------------------
    if not args.dry:
        log.debug('Copying across input_cards...')
        call(['hadoop', 'fs', '-copyFromLocal', '-f',
              'input_cards', args.oDir.replace('/hdfs', '')])
    copy_to_local[os.path.join(args.oDir, 'input_cards')] = 'input_cards'

    # Copy run script to outputdir to sandbox it
    # -------------------------------------------------------------------------
    sandbox_script = os.path.join(args.oDir, 'run_mg5.py')
    copy_to_local[sandbox_script] = 'run_mg5.py'
    if not args.dry:
        log.debug('Copying across exe...')
        shutil.copy2('run_mg5.py', sandbox_script)

    # Setup log directory
    # -------------------------------------------------------------------------
    log_dir = '%s/logs' % generate_subdir(args.channel, args.energy)
    check_create_dir(log_dir)

    # File stem common for all dag and status files
    # -------------------------------------------------------------------------
    file_stem = os.path.join(generate_subdir(args.channel, args.energy),
                             strftime("%H%M%S"))
    check_create_dir(os.path.dirname(file_stem))

    # Make DAG file
    # -------------------------------------------------------------------------
    dag_name = file_stem + '.dag'
    status_name = file_stem + '.status'
    write_dag_file(dag_filename=dag_name,
                   condor_filename='HTCondor/mcJob.condor',
                   status_filename=status_name,
                   copyToLocal=copy_to_local, copyFromLocal=copy_from_local,
                   log_dir=log_dir, args=args)

    # Submit it
    # -------------------------------------------------------------------------
    if args.dry:
        log.warning('Dry run - not submitting jobs or copying files.')
    else:
        call(['condor_submit_dag', dag_name])
        log.info('Check status with:')
        log.info('DAGstatus.py %s' % status_name)
        log.info('Condor log files written to: %s' % log_dir)
        print''


def write_dag_file(dag_filename, condor_filename, status_filename, log_dir,
                   copyToLocal, copyFromLocal, args):
    """Write a DAG file for a set of jobs.

    Creates a DAG file, adding extra flags for the worker node script.
    This includes setting the random number generator seed, and copying files
    to & from /hdfs. Also ensures a DAG status file will be written every 30s.

    dag_filename: str
        Name to be used for DAG job file.
    condor_filename: str
        Name of condor job file to be used for each job.
    status_filename: str
        Name to be used for DAG status file.
    copyToLocal: dict{str : str}
        Dict of things to copyToLocal when the worker node script starts.
        Of the form source : destination
    copyFromLocal: dict{str : str}
        Dict of things to copyFromLocal when the worker node script ends.
        Of the form source : destination
    args: argparse.Namespace
        Contains info about output directory, job IDs, number of events per job,
        and args to pass to the executable.
    """
    # to parse the MG5 specific parts
    mg5_parser = MG5ArgParser()
    mg5_args = mg5_parser.parse_args(args.args)

    log.info("DAG file: %s" % dag_filename)
    with open(dag_filename, 'w') as dag_file:
        dag_file.write('# DAG for channel %s\n' % args.channel)
        dag_file.write('# Outputting to %s\n' % args.oDir)
        for job_ind in xrange(args.jobIdRange[0], args.jobIdRange[1] + 1):
            # add job to DAG
            job_name = '%d_%s' % (job_ind, args.channel)
            dag_file.write('JOB %s %s\n' % (job_name, condor_filename))

            # args to pass to the script on the worker node
            job_opts = []

            # start with files to copyToLocal at the start of job running
            # ----------------------------------------------------------------
            if copyToLocal:
                for src, dest in copyToLocal.iteritems():
                    job_opts.extend(['--copyToLocal', src, dest])

            mg5_args.iseed = job_ind  # RNG seed using job index

            # Make sure output files are copied across afterwards
            # ----------------------------------------------------------------
            output_dir = os.path.join(args.channel, 'Events', 'run_01')
            name_stem = '%s_%dTeV_n%d_seed%d' % (args.channel, args.energy,
                                                 mg5_args.nevents, mg5_args.iseed)

            lhe_zip = os.path.join(output_dir, 'events.lhe.gz')
            lhe_final_zip = '%s.lhe.gz' % name_stem

            hepmc_zip = os.path.join(output_dir, 'events_PYTHIA8_0.hepmc.gz')
            hepmc_final_zip = '%s.hepmc.gz' % name_stem

            job_opts.extend(['--copyFromLocal', lhe_zip, os.path.join(args.oDir, 'lhe', lhe_final_zip)])
            job_opts.extend(['--copyFromLocal', hepmc_zip, os.path.join(args.oDir, 'hepmc', hepmc_final_zip)])
            # Supplementary materials
            job_opts.extend(['--copyFromLocal', os.path.join(output_dir, 'RunMaterial.tar.gz'),
                             os.path.join(args.oDir, 'other', 'RunMaterial_%d.tar.gz' % job_ind)])
            job_opts.extend(['--copyFromLocal', os.path.join(output_dir, 'summary.txt'),
                             os.path.join(args.oDir, 'other', 'summary_%d.txt' % job_ind)])

            # add in any other files that should be copied from the worker at
            # the end of the job
            # ----------------------------------------------------------------
            if copyFromLocal:
                for src, dest in copyFromLocal.iteritems():
                    job_opts.extend(['--copyFromLocal', src, dest])

            job_opts.append('--args')
            for k, v in mg5_args.__dict__.items():
                if k and v:
                    job_opts.extend(['--' + str(k), str(v)])

            # make some replacements due to different destination variable name
            # screwing things up. Yuck!
            remap = {'--iseed': '--seed', '--pythia8_path': '--pythia8'}
            for k, v in remap.items():
                job_opts[job_opts.index(k)] = v
            job_opts.remove('--card')
            log.debug('job_opts: %s' % job_opts)

            # write job vars to file
            dag_file.write('VARS %s ' % job_name)
            log_name = os.path.splitext(os.path.basename(dag_filename))[0]
            dag_file.write('opts="%s" logdir="%s" logfile="%s"\n' % (' '.join(job_opts),
                                                                     log_dir,
                                                                     log_name))
        dag_file.write('NODE_STATUS_FILE %s 30\n' % status_filename)


def check_create_dir(directory):
    """Check to see if directory exists, if not make it.

    Can optionally display message to user.
    """
    if not os.path.isdir(directory):
        if os.path.isfile(directory):
            raise RuntimeError("Cannot create directory %s, already "
                               "exists as a file object" % directory)
        os.makedirs(directory)
        log.debug("Making dir %s" % directory)


def generate_subdir(channel, energy=13):
    """Generate a subdirectory name using channel and date.
    Can be used for output and log files, so consistent between both.

    >>> generate_subdir('ggh_4tau', 8)
    8TeV/ggh_4tau/05_Oct_15
    """
    return os.path.join('%dTeV' % energy, channel, strftime("%d_%b_%y"))


def generate_dir_soolin(channel, energy=13):
    """Generate a directory name on /hdfs using userId, channel, and date.

    >>> generate_dir_soolin('ggh_4tau', 8)
    /hdfs/user/<username>/NMSSMPheno/MG5_aMC/8TeV/ggh_4tau/<date>
    """
    uid = getpass.getuser()
    return "/hdfs/user/%s/NMSSMPheno/MG5_aMC/%s" % (uid, generate_subdir(channel, energy))


def frange(start, stop, step=1.0):
    """Generate an iterator to loop over a range of floats."""
    i = start
    while i <= stop:
        yield i
        i += step


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
    submit_mc_jobs_htcondor()
