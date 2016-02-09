#!/usr/bin/env python
"""
Script to run MG5_aMC locally. Creates new input card from user's options,
to ensure that Pythia8 & HepMC linked correctly, and other options.
"""

import argparse
import sys
import os
import re
import logging
from subprocess import check_call
import shutil


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


class MG5ArgParser(argparse.ArgumentParser):
    """
    Class to handle parsing of options. This allows it to be used in other
    scripts (e.g. for HTCondor/PBS batch scripts)
    """

    def __init__(self, *args, **kwargs):
        super(MG5ArgParser, self).__init__(*args, **kwargs)
        self.add_arguments()

    def add_arguments(self):
        self.add_argument('card',
                          help='card file to pass to MG5_aMC')
        self.add_argument('--exe',
                          help='Location of mg5_aMC executable',
                          default="mg5_aMC")
        self.add_argument('-n', '--nevents',
                          help='Number of events to generate',
                          type=int)
        self.add_argument('--seed',
                          dest='iseed',
                          help='Random number generator seed',
                          default=0,
                          type=int)
        self.add_argument('--pythia8',
                          dest='pythia8_path',
                          help='Path to Pythia directory',
                          required=True)
        self.add_argument('--hepmc',
                          help='Path to HepMC install directory',
                          required=True)
        self.add_argument('--dry',
                          action='store_true',
                          help="Only make card, don't run MG5_aMC")
        self.add_argument('--new',
                          help='Filename for new card. '
                          'If not specified, defaults to <card>_new.txt')
        self.add_argument('--newstem',
                          help='Stem for new output filenames.')
        self.add_argument("-v",
                          action='store_true',
                          help="Display debug messages.")
    # self.add_argument('--option',
    #                     nargs=2,
    #                     action='append',
    #                     help='Allow replacement of any other options. '
    #                     'Specify as: --option OPTION VALUE, for as many '
    #                     'options as you wish to change '
    #                     'e.g. --option ickkw 3 --option Qcut 15')


def run_mg5(in_args=sys.argv[1:]):
    """Main function to create a new card for MG5_aMC and run it."""

    parser = MG5ArgParser(description=__doc__)
    args = parser.parse_args(in_args)
    if args.v:
        log.setLevel(logging.DEBUG)
        log.debug(args)

    # Use script args to create dict of fields to replace in card
    if 'hepmc' in args:
        args.__dict__['extrapaths'] = "../lib %s" % os.path.join(os.path.realpath(args.hepmc), 'lib')
        args.__dict__['includepaths'] = os.path.join(os.path.realpath(args.hepmc), 'include')

    if 'pythia8_path' in args:
        args.__dict__['pythia8_path'] = os.path.realpath(args.pythia8_path)

    mg_vars = ['nevents', 'iseed', 'pythia8_path', 'extrapaths', 'includepaths']
    fields = {k: args.__dict__[k] for k in mg_vars if args.__dict__[k]}

    log.debug(fields)

    # make a new card for MG5_aMC
    new_card = os.path.splitext(args.card)[0] + '_new' + os.path.splitext(args.card)[1]
    if args.new:
        new_card = args.new
    make_card(args.card, new_card, fields)

    # run MG5_aMC
    if not args.dry:
        log.info('Running MG5_aMC with card %s' % new_card)
        mg5_cmds = [os.path.realpath(args.exe), new_card]
        log.debug(mg5_cmds)
        check_call(mg5_cmds)

        if args.newstem:
            # rename output files to avoid generic names - can MG do this already?
            channel = get_value_from_card(new_card, 'output')
            print os.listdir(channel)
            print os.listdir(channel + '/Events')
            print os.listdir(channel + '/Events/run_01')

            output_dir = os.path.join(channel, 'Events', 'run_01')
            shutil.move(os.path.join(output_dir, 'events.lhe.gz'),
                        os.path.join(output_dir, args.newstem + '.lhe.gz'))
            shutil.move(os.path.join(output_dir, 'events_PYTHIA8_0.hepmc.gz'),
                        os.path.join(output_dir, args.newstem + '.hepmc.gz'))
            shutil.move(os.path.join(output_dir, 'RunMaterial.tar.gz'),
                        os.path.join(output_dir, 'RunMaterial_' + args.newstem + '.tar.gz'))
            shutil.move(os.path.join(output_dir, 'summary.txt'),
                        os.path.join(output_dir, 'summary_' + args.newstem + '.txt'))
    return 0


def make_card(in_card, out_card, fields):
    """Make a copy of a card file, replacing various attributes.

    The attribute replacement is made if a line contains a variable name as
    a word surrounded by spaces.

    Parameters
    ----------
    in_card : str
        Name of card to use as template.
    out_card : str
        Name of card to produce.
    fields : dict
        Dict of thing to replace in the template card. Dict should be of the
        form, {<var_name>: <value>}. var_name must be a str.
        <var_name> is the name of a MG5 variable.
        <value> is the value of that variable.
        A replacement will occur if the line contains the <var_name> as a word.
        So if <var_name> = "nevents", "set run_card nevents 200" would match,
        but "output genevents" would not match.

    For example:
    >>> fields = {'nevents': '200', 'output': 'new_process'}
    >>> make_card('old_card.txt', 'new_card.txt', fields)
    """
    with open(in_card) as in_file:
        card_template = in_file.readlines()  # hmm read or readlines

    for name, value in fields.iteritems():
        log.debug('Setting %s for %s' % (value, name))
        p = re.compile(name + r' (.*)$')

        for i, line in enumerate(card_template):
            # skip empty or comment lines
            if not line or line.startswith('#') or name not in line:
                continue
            try:
                # get old values to replace
                old_values = p.search(line).group(1)
            except IndexError:
                log.error('Error finding line with %s in card' % name)
                raise
            card_template[i] = line.replace(old_values, str(value))

    log.info('Writing new card to %s' % out_card)
    log.debug(''.join(card_template))
    with open(out_card, 'w') as out_file:
        out_file.write(''.join(card_template))


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
    sys.exit(run_mg5())
