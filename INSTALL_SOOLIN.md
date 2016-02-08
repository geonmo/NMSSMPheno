#Installation instruction for running on Soolin at Bristol

Do it in the following order, as MG5_aMC@NLO depends on Pythia8.

- [Pythia8 (signal MC)](#pythia8-signal-mc)
- [MG5_aMC@NLO (background MC)](#mg5_amcnlo-background-mc)
- [Delphes (detector simulation)](#delphes-detector-simulation)
- [MadAnalysis (analysis)](#madanalysis-analysis)

**VERY IMPORTANT**: do all of this inside the CMSSW_7_4_4_ROOT5 release. It has all the correct GCC, ROOT, etc setup:

```
cmsrel CMSSW_7_4_4_ROOT5
```

**ALSO IMPORTANT**: to run the HTCondor submission scripts, you will need to install the [`htcondenser`](https://github.com/raggleton/htcondenser) library.

**NB**: all steps below assume you are installing things into /users/$LOGNAME (i.e. $HOME). If this is not the case, adjust accordingly.

##Pythia8 (signal MC)

First install:

- HepMC
- FastJet
- LHAPDF6

BOOST and ROOT5 are also required, but we can use the central installations instead.

###HepMC

```
wget http://lcgapp.cern.ch/project/simu/HepMC/download/HepMC-2.06.09.tar.gz -O- | tar xvz
mkdir -p HepMC/build HepMC/install
cd HepMC/build
../../HepMC-2.06.09/configure --prefix=${PWD}/../install/ --with-momentum=GEV --with-length=MM
make -j4
make check
make install
```

###FastJet



###LHAPDF6

```
wget http://www.hepforge.org/archive/lhapdf/LHAPDF-6.1.5.tar.gz -O- | tar xvz
mkdir LHAPDF6
cd LHAPDF-6.1.5
./configure --prefix=${PWD}/../LHAPDF6
make -j4
make install
```

Note that LHAPDF does not come with the PDF sets - they must be downloaded separately. For example, to add the CT10NLO PDF:

```
 wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.1/CT10nlo.tar.gz -O- | tar xz -C $HOME/LHAPDF6/share/LHAPDF
```


###Pythia8

Download Py8 tarball: [http://home.thep.lu.se/~torbjorn/Pythia.html](http://home.thep.lu.se/~torbjorn/Pythia.html) and extract.
I put the extracted folder in $HOME/pythia8/, but feel free to put it anywhere else, just update instructions accordingly.

Compile the libraries. Note that this assumes HepMC and LHAPDF are installed in $HOME - change if necessary:

```
./configure --with-hepmc2=$HOME/HepMC/install/ \
--with-fastjet3=$HOME/fastjet-install \
--with-boost-include=/cvmfs/cms.cern.ch/slc6_amd64_gcc491/external/boost/1.57.0-cms/include/ \
--with-boost-lib=/cvmfs/cms.cern.ch/slc6_amd64_gcc491/cms/cmssw/CMSSW_7_4_4_ROOT5/external/slc6_amd64_gcc491/lib \
--with-gzip \
--cxx-common='-D__USE_XOPEN2K8 -fPIC' \
--with-lhapdf6=$HOME/LHAPDF6-install/ --with-lhapdf6-plugin=include/Pythia8Plugins/LHAPDF6.h \
--with-root=/cvmfs/cms.cern.ch/slc6_amd64_gcc491/lcg/root/5.34.22-ilphmn/
make
```

Afterwards, set:

```
export PYTHIA8DATA=$HOME/Pythia8/pythia8215/share/Pythia8/xmldoc/
```
to avoid the error message:

```
PYTHIA Abort from Pythia::Pythia: unmatched version numbers : in code 8.215 but in XML 8.205
```

And set

```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/HepMC/install/lib
```
to avoid the error message:

```
./Pythia8.exe: error while loading shared libraries: libHepMC.so.4: cannot open shared object file: No such file or directory
```

Test the libraries have compiled correctly by compiling & running example programs:

```
cd examples
make mainXX
./mainXX
```

- `main01`: very basic, no dependencies
- `main41`: test HepMC linked correctly
- `main52`: test LHAPDF linked correctly. Note that this example still uses LHAPDF5, so you will need to manually change:

```
string pdfSet = "LHAPDF5:MRST2001lo.LHgrid";
\\ change to
string pdfSet = "LHAPDF6:CT10nlo.LHgrid";
```

You should see when running:

```
LHAPDF 6.1.5 loading $HOME/LHAPDF6-install/share/LHAPDF/CT10nlo/CT10nlo_0000.dat
CT10nlo PDF set, member #0, version 4; LHAPDF ID = 11000
```

- `main91`: test ROOT linked correctly

##MG5_aMC@NLO (background MC)

Download tar into $HOME/zips. **This will be needed for submitting batch jobs to HTCondor**.
Extract, and create a symlink to this directory:

```
ln -s <mg5 dir>/bin/mg5_aMC mg5_aMC
```

Test it runs OK interactively by running:

```
./mg5_aMC input_cards/pp13_bb.txt
```

##Delphes (detector simulation)

Assumes ROOT5 already installed. (**NEW**: Delphes 3.3.0 supports root6. Dunno whether to use it or not...)

```
git clone git@github.com:delphes/delphes.git
cd delphes
make -j4
```

##MadAnalysis (analysis)

**IMPORTANT**: to compile the Delphes addition, you must use
```
export SCRAM_ARCH=slc6_amd64_gcc472
```
and use `CMSSW_6_2_12`. To be fixed?!

Download tar. You should be able to run it out of the box:

```
./bin/ma5
```

Make sure to run setup.sh each time!