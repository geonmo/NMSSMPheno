#NMSSMPheno

For NMSSM phenomenology studies.

Basic overview:

1) Make MC complete with showering using Pythia8 (for signal processes) or MG5_aMC@NLO (for SM processes).

2) Run through Delphes for a detector simulation based around CMS (TODO for PBS)

3) Analyse with MadAnlaysis (TODO for HTCondor/PBS)

This system has been designed to run on various systems:
- Soolin at Bristol (HTCondor batch system)
- Iridis at Southampton (PBS batch system)

##Installation

- Get [`htcondenser`](https://github.com/raggleton/htcondenser). Run `setup.sh`

- Clone this repository:

```
git clone git@github.com:raggleton/NMSSMPheno.git
```

- Follow the program installation instructions for the system you are using:
    - Soolin @ Bristol: [INSTALL_SOOLIN.md](INSTALL_SOOLIN.md)
    - Iridis @ Soton: [INSTALL_IRIDIS.md](INSTALL_IRIDIS.md)

**TODO**: some automated way of installing and compiling everything.