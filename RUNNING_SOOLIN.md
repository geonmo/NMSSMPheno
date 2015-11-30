#RUNNING_SOOLIN

Instructions for running the complete chain on Soolin, using the HTCondor batch system.

There are a few steps:

1) Generate Monte Carlo events for signal or SM processes

2) Apply detector simulation

3) Analyse it all

##Generate Monte Carlo

SM processes are generated with MadGraph5_aMC@NLO, whilst SUSY processes are generated with Pythia8. There are slightly different instructions for each.

###Standard Model: MG5_aMC@NLO

Use the script [run_mg5.py](MG5_aMC/run_mg5.py). This is a very simple wrapper - basically auto-creates a new card using a template, and user-specified options about Pythia8 and HepMC install directories.

####Running batch jobs on HTCondor

Use the script [submit_mg5_jobs_htcondor.py](MG5_aMC/submit_mg5_jobs_htcondor.py). Show possible option using the `--help` flag. **Important** before submitting jobs, you must have extracted and run MG5_aMC locally. In addition, you must set the variable `MG5_DIR` in [submit_mg5_jobs_htcondor.py](MG5_aMC/submit_mg5_jobs_htcondor.py) to point to your MG5 installation directory.

###SUSY: Pythia8

One program is responsible for running Pythia8, [generateMC.cc](Pythia/src/generateMC.cc). The exact process(es) are specified using an input card, see [input_cards](Pythia/input_cards). Common options (e.g. beam parameters) are specified in [common_pp13.cmnd](Pythia/input_cards/common_pp13.cmnd).

To compile and produce the executable:

```
cd Pythia
make
```

Then to run it:

```
./generateMC.exe <opts>
```

See a list of options by using the `--help` flag. **As a minimum** you will need the name of an input card. There are also options for changing the number of events to generate, changing the mass of the a1, as well as several output formats. They all take optional filenames. If no filename is specified, then one will be auto-generated based upon the input card, mass, and random number generator seed.

- `--hepmc`: saves the **complete event listing (including hadronisation)** in HepMC format. Suitable for passing to Delphes.
- `--lhe`: saves the **hard process only** in LHE format. Suitable for passing to another MC program to hadronise, or to study the hard event itself.
- `--root`: saves user-defined histograms to a ROOT file. The user must define the histogram objects, and can then fill them by analysing the Pythia event object. This is done in [generateMC.cc](Pythia/src/generateMC.cc). An example of quick plot-making is done in [deltaR_studies](Pythia/deltaR_studies).

####Running batch jobs on HTCondor

Use the script [submit_py8_jobs_htcondor.py](Pythia/submit_py8_jobs_htcondor.py). Show possible option using the `--help` flag. **As a minimum** you will need to specify the range of job IDs to run over. The job ID is also the random number seed, so you must ensure that they differ. There are also optional arguments for specifying output directory, using a different executable, etc. You can also pass the options that `generateMC.exe` uses by using the `--args` flag. You must specify, as a minimum, the input card, and mass of a1 (if you are not using the `--massRange` option). Each job will generate the same number of events, as specified using the `-n|--number` flag.

There is also the option of submitting jobs for a range of a1 masses. This is accomplished by using the `--massRange` flag and providing the start/end masses, and the mass interval. For each mass point, jobs with the job IDs specified will be submitted.

For example, submitting 3 jobs, each of 10k events, using the [ggh125_2a_4tau.cmnd](Pythia/input_cards/ggh125_2a_4tau.cmnd) card, saving the output to hepmc and root formats to the default dir on hdfs (`/hdfs/users/$LOGNAME/NMSSMPheno/Pythia8/<card>/<date>`), with ma1 = [4, 6, 8]:

```
./submit_mc_jobs_htcondor.py 1 3 --massRange 4 8 2 \
--args --card input_cards/ggh125_2a_4tau.cmnd -n 10000 --hepmc --root
```

This will take approximately 10 minutes. The resultant HepMC file will be ~ 1.9 GB in size.

##Apply detector simulation

Detector simulation is applied using Delphes. We pass it a HepMC file as generated in the previous step, and a card specifying the detector configuration.

For example:

```
./DelphesHEPMC <card file> <output ROOT filename> <input file(s) in LHE, HepMC format>
```

**TODO**

###Running batch jobs on HTCondor

Use the script [submit_delphes_jobs_htcondor.py](Delphes/submit_delphes_jobs_htcondor.py).

##Analysis

**TODO**

###Running batch jobs on HTCondor

**TODO**