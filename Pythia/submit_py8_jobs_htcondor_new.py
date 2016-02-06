#!/usr/bin/env python
"""
Submit lots of Pythia8 jobs to condor. All jobs run using the same input card.

The user must specify the range of job IDs to submit. The job ID also sets the
random number generator seed, thus one should avoid using the same job ID more
than once. The user can also specify the output directory
(or one will be auto-generated), as well as a custom executable. There is also
the possibiility of looping over a range of masses, in which case job IDs
(as specified the jobIdRange arguments) will be used for each mass.

To pass arguments to the Pythia8 program, use the '--args' flag.
e.g. if you ran locally with:
'--card mycard.txt --mass 8 -n 10000'
you should call this script with:
'--args --card mycard.txt --mass 8 -n 10000'

Note that --args must be specified after all other arguments!

There is also the option for a 'dry run' where all the files & directories are
set up, but the job is not submitted.

Note that this submits the jobs not one-by-one but as a DAG, to allow easier
monitoring of job status.
"""


from time import strftime
import argparse
import sys
sys.path.append('../Common')
import common
import os
import getpass
import logging
import htcondenser as ht


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

# Set directory for STDOUT/STDERR/LOG from jobs
LOG_DIR = '/storage/%s/NMSSMPheno/Pythia8' % os.environ['LOGNAME']


def submit_mc_jobs_htcondor(in_args=sys.argv[1:], log_dir=LOG_DIR):
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
                        nargs=2, type=int)  # no metavar, bug with positional args
    parser.add_argument("--oDir",
                        help="Directory for output HepMC files. "
                        "If no directory is specified, an automatic one will "
                        "be created at: "
                        "/hdfs/user/<username>/NMSSMPheno/Pythia8/<energy>TeV/<card>/<date>",
                        default="")
    parser.add_argument("--exe",
                        help="Executable to run.",
                        default="generateMC.exe")
    parser.add_argument("--massRange",
                        help="Specify mass range to run over. "
                        "Must be of the form: startMass, endMass, massStep. "
                        "For each mass point, njobs jobs will be submitted. "
                        "This will superseed any --mass option passed via --args",
                        nargs=3, type=float,
                        metavar=('startMass', 'endMass', 'massStep'))
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

    log.info('>>> Creating jobs')

    if args.v:
        log.setLevel(logging.DEBUG)

    log.debug('program args: %s', args)

    # Do some sanity checks
    check_args(args)

    # Get the input card from user's options & check it exists
    try:
        card = get_option_in_args(args.args, "--card")
    except KeyError:
        log.exception('You did not specify an input card!')
    if not card:
        raise RuntimeError('You did not specify an input card!')
    if not os.path.isfile(card):
        raise RuntimeError('Input card %s does not exist!' % card)
    args.card = card
    args.channel = os.path.splitext(os.path.basename(card))[0]

    # Make sure output zipped
    if '--zip' not in args.args:
        args.args.append("--zip")

    # Get CoM energy
    try:
        args.energy = int(get_option_in_args(args.args, '--energy'))
    except KeyError:
        args.energy = 13

    # Loop over required mass(es), generating DAG files for each
    if args.massRange:
        masses = common.frange(args.massRange[0], args.massRange[1], args.massRange[2])
    else:
        masses = [get_option_in_args(args.args, '--mass')]

    status_files = []

    for mass in masses:
        # Auto generate output directory if necessary
        if args.oDir == "":
            args.oDir = generate_dir_soolin(args.channel, args.energy, mass)

        # Setup log directory
        log_dir = '%s/%s/logs' % (log_dir, generate_subdir(args.channel, args.energy, mass))

        # File stem common for all dag and status files
        file_stem = '%s/py8_%s' % (generate_subdir(args.channel, args.energy, mass),
                                   strftime("%H%M%S"))
        common.check_create_dir(os.path.dirname(file_stem))

        # Make DAGMan
        status_name = file_stem + '.status'
        status_files.append(status_name)
        pythia_dag = create_dag(dag_filename=file_stem + '.dag',
                                condor_filename='HTCondor/pythia.condor',
                                status_filename=status_name,
                                log_dir=log_dir, mass=mass, args=args)

        # Submit it
        if args.dry:
            log.warning('Dry run - not submitting jobs or copying files.')
            pythia_dag.write()
        else:
            pythia_dag.submit()

    if len(status_files) > 1:
        log.info('Check all statuses with:')
        log.info('DAGstatus.py %s', ' '.join(status_files))

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
        If exe does not exist
        If jobIdRange invalid
    """
    if not os.path.isfile(args.exe):
        raise RuntimeError('Executable %s does not exist' % args.exe)

    if args.jobIdRange[0] < 1:
        raise RuntimeError('The first jobIdRange argument must be >= 1.')

    if args.jobIdRange[1] < args.jobIdRange[0]:
        raise RuntimeError('The second jobIdRange argument must be >= the first.')

    if args.massRange:
        if any(x <= 0 for x in args.massRange):
            raise RuntimeError('You cannot have a mass <= 0')
        if args.massRange[1] < args.massRange[0]:
            raise RuntimeError('You cannot have endMass < startMass')


def create_dag(dag_filename, status_filename, condor_filename, log_dir, mass, args):
    """Create a htcondenser.DAGMan to run a set of Pythia8 jobs.

    Parameters
    ----------
    dag_filename: str
        Name to be used for DAG job file.
    condor_filename: str
        Name of condor job file to be used for each job.
    status_filename: str
        Name to be used for DAG status file.
    mass: int, float
        Mass of a1 boson. Used to auto-generate HepMC filename.
    args: argparse.Namespace
        Contains info about output directory, job IDs, number of events per job,
        and args to pass to the executable.

    """

    num_events = get_number_events(args)

    # set mass in args passed to program
    if '--mass' in args.args:
        set_option_in_args(args.args, '--mass', mass)
    else:
        args.args.extend(['--mass', mass])

    log.debug('args.args before: %s', args.args)

    pythia_jobset = ht.JobSet(exe=args.exe, copy_exe=True,
                              setup_script='HTCondor/setup.sh',
                              filename=condor_filename,
                              out_dir=log_dir, err_dir=log_dir, log_dir=log_dir,
                              memory="100MB", disk="2GB", share_exe_setup=True,
                              common_input_files=[args.card, 'input_cards/common_pp.cmnd'],
                              hdfs_store=args.oDir)

    pythia_dag = ht.DAGMan(filename=dag_filename, status_file=status_filename)

    for job_ind in xrange(args.jobIdRange[0], args.jobIdRange[1] + 1):
        pythia_job = generate_pythia_job(args, job_ind, mass, num_events)
        pythia_jobset.add_job(pythia_job)
        pythia_dag.add_job(pythia_job)

    return pythia_dag


def generate_subdir(channel, energy=13, mass=0):
    """Generate a subdirectory name.
    Can be used for output and log files, so consistent between both.

    >>> generate_subdir('ggh_4tau', energy=8, mass=4)
    8TeV_ggh_4tau_m4/05_Oct_15
    """
    mass = str(mass)
    return os.path.join('%s_mass%s_%dTeV' % (channel, mass, energy), strftime("%d_%b_%y"))
    # return os.path.join('%dTeV' % energy, channel, 'mass%s' % mass, strftime("%d_%b_%y"))


def generate_dir_soolin(channel, energy=13, mass=0):
    """Generate a directory name on /hdfs using userId, channel, and date.

    >>> generate_dir_soolin('ggh_4tau', 13, 8)
    /hdfs/user/<username>/NMSSMPheno/Pythia8/13TeV_ggh_4tau_m8/<date>
    """
    uid = getpass.getuser()
    return "/hdfs/user/%s/NMSSMPheno/Pythia8/%s" % (uid, generate_subdir(channel, energy, mass))


def generate_filename(channel, mass, energy, num_events, fmt):
    """Centralised filename generator using various info"""
    return "%s_mass%s_%dTeV_n%s.%s" % (channel, str(mass), energy, str(num_events), fmt)


def get_option_in_args(args, flag):
    """Return value that accompanied flag in list of args.

    Will return None if there is no accompanying value, and will raise a
    KeyError if the flag does not appear in the list.

    >>> args = ['--foo', 'bar', '--man']
    >>> get_option_in_args(args, "--foo")
    bar
    >>> get_option_in_args(args, "--man")
    None
    >>> get_option_in_args(args, "--fish")
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "submit_mc_jobs_htcondor.py", line 272, in get_option_in_args
        raise KeyError('%s not in args' % flag)
    KeyError: '--fish not in args'
    """
    # maybe a dict would be better for this and set_option_in_args()?
    if flag not in args:
        raise KeyError('%s not in args' % flag)
    if flag == args[-1]:
        return None
    val = args[args.index(flag) + 1]
    if val.startswith('-'):
        return None
    return val


def set_option_in_args(args, flag, value):
    """Set value for flag in list of args.

    If no value already exists, it will insert the value after the flag.
    If the flag does not appear in args, a KeyError will be raised.

    >>> args = ['--foo', 'bar', '--man', '--pasta']
    >>> set_option_in_args(args, '--foo', 'ball')
    >>> args
    ['--foo', 'ball', '--man', '--pasta']
    >>> set_option_in_args(args, '--man', 'trap')
    >>> args
    ['--foo', 'ball', '--man', 'trap', --pasta']
    >>> set_option_in_args(args, '--pasta', 'bake')
    >>> args
    ['--foo', 'ball', '--man', 'trap', --pasta', 'bake']

    """
    # first check if a value already exists.
    if get_option_in_args(args, flag):
        args[args.index(flag) + 1] = value
    else:
        if flag == args[-1]:  # if the flag is the last entry in args
            args.append(value)
        elif args[args.index(flag) + 1].startswith('-'):
            # if the next entry is a flag, we need to insert our value
            args.insert(args.index(flag) + 1, value)


def get_number_events(args):
    """Return number of events as specified in user args.

    Parameters
    ----------
    args : argparse.Namespace

    Returns
    -------
    int
        Number of events. Default 1 is not specified
    """
    if '--number' in args.args:
        return int(get_option_in_args(args.args, "--number"))
    elif '-n' in args.args:
        return int(get_option_in_args(args.args, "-n"))
    else:
        log.warning('Number of events per job not specified - assuming 1')
        return 1


def generate_pythia_job(args, job_index, mass, num_events):
    """
    Parameters
    ----------
    args : TYPE
        Description
    job_index : TYPE
        Description
    mass : TYPE
        Description
    num_events : TYPE
        Description

    Returns
    -------
    """
    exe_args = args.args[:]
    exe_args.extend(['--seed', job_index])  # RNG seed using job index

    out_files = []

    # Sort out output files. Ensure that they have the seed appended to
    # filename, and that they will be copied to hdfs afterwards.
    for fmt in ['hepmc', 'root', 'lhe']:
        # special warning for hepmc files
        flag = '--%s' % fmt
        if fmt == "hepmc" and flag not in exe_args:
            log.warning("You didn't specify --hepmc in your list of --args. "
                        "No HepMC file will be produced.")
        if flag not in exe_args:
            continue
        else:
            # Auto generate output filename if necessary
            # Bit hacky as have to manually sync with PythiaProgramOpts
            if not get_option_in_args(args.args, flag):
                out_name = generate_filename(args.channel, mass, args.energy, num_events, fmt)
                set_option_in_args(exe_args, flag, out_name)

            # Use the filename itself, ignore any directories from user.
            out_name = os.path.basename(get_option_in_args(exe_args, flag))

            # Add in seed/job ID to filename. Note that generateMC.cc adds the
            # seed to the auto-generated filename, so we only need to modify it
            # if the user has specified the name
            out_name = "%s_seed%d.%s" % (os.path.splitext(out_name)[0],
                                         job_index, fmt)
            set_option_in_args(exe_args, flag, out_name)
            if '--zip' in exe_args:
                out_name += ".gz"
            out_files.append(out_name)

    pythia_job = ht.Job(name='%d_%s' % (job_index, args.channel),
                        args=exe_args, output_files=out_files,
                        hdfs_mirror_dir=args.oDir)
    return pythia_job

if __name__ == "__main__":
    sys.exit(submit_mc_jobs_htcondor())
