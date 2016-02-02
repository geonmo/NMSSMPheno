#include "PythiaProgramOpts.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

namespace fs = boost::filesystem;

using std::cerr;
using boost::lexical_cast;

PythiaProgramOpts::PythiaProgramOpts(int argc, char* argv[]):
  cardName_(""),
  nEvents_(1),
  mass_(8.),
  seed_(0),
  energy_(13),
  diMuFilter_(false),
  writeToHEPMC_(false),
  filenameHEPMC_(""),
  writeToLHE_(false),
  filenameLHE_(""),
  writeToROOT_(false),
  filenameROOT_(""),
  printEvent_(false),
  verbose_(false),
  zip_(false),
  desc_("\nProduces MC for p-p collisions.\n"
    "User must specify the physics process(es) to be generated \nvia an input"
    " card (see input_cards directory for examples).\nDefaults for beams, "
    "PDF, etc are set in input_cards/common_pp.cmnd\n\nAllowed options:")
{
  desc_.add_options()
    ("help,h", "Produce help message")
    ("card", po::value<std::string>(&cardName_)->required(),
      "Name of Pythia8 settings card to loads physics processes")
    ("number,n", po::value<int>(&nEvents_)->default_value(nEvents_),
      "Number of events to run over [default = 1]. ")
    ("mass", po::value<double>(&mass_)->default_value(mass_),
      "Mass of a1 boson in GeV")
    ("seed", po::value<int>(&seed_)->default_value(seed_),
      "Seed for random number generator. 0 = uses time. " \
      "WARNING: DON'T USE 0 FOR BATCH SYSTEM. " \
      "Get simultaneous start = same seed = same events. " \
      "Set seed explicitly instead (e.g. file number).")
    ("energy", po::value<double>(&energy_)->default_value(energy_),
      "Center-of-mass energy (in TeV).")
    ("diMuFilter", po::bool_switch(&diMuFilter_)->default_value(diMuFilter_),
      "Enable di-muon filter, so events are guaranteed to have >=2 final state muons.")
    ("hepmc", po::value<std::string>(&filenameHEPMC_)->implicit_value(filenameHEPMC_),
      "Save output in HepMC format (includes hadronisation). " \
      "Can optionally take a filename for the HepMC file. "\
      "If you don't provide a filename, " \
      "the default filename will be <card>_ma1_<mass>_<seed>.hepmc")
    ("lhe", po::value<std::string>(&filenameLHE_)->implicit_value(filenameLHE_),
      "Save output in LHE format (hard process only). " \
      "Can optionally take a filename for the LHE file. "\
      "If you don't provide a filename, " \
      "the default filename will be <card>_ma1_<mass>_<seed>.lhe")
    ("root", po::value<std::string>(&filenameROOT_)->implicit_value(filenameROOT_),
      "Save plots to ROOT file. " \
      "Can optionally take a filename for the ROOT file. "\
      "If you don't provide a filename, " \
      "the default filename will be <card>_ma1_<mass>_<seed>.root")
    ("printEvent", po::bool_switch(&printEvent_)->default_value(printEvent_),
      "Prints complete event listing of first event to screen")
    ("verbose,v", po::bool_switch(&verbose_)->default_value(verbose_),
      "Output debugging statements")
    ("zip", po::bool_switch(&zip_)->default_value(zip_),
      "Compress LHE and HepMC outputs using gzip")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc_), vm);

  // put this before po::notify otherwise error thrown when just using --help
  if (vm.count("help")) {
    cout << desc_ << endl;
    exit(1);
  }

  po::notify(vm);

  // Check input card exists
  if (!fs::exists(fs::path(cardName_))) {
    throw std::runtime_error("Input card \"" + cardName_+ "\" does not exist");
  }

  // Handle output filenames for various formats
  if (vm.count("hepmc")) {
    writeToHEPMC_ = true;
    // Generate default filename if necessary
    if (filenameHEPMC_ == "") {
      filenameHEPMC_ = generateFilenameStem() + ".hepmc";
    }
  }

  if (vm.count("lhe")) {
    writeToLHE_ = true;
    // Generate default filename if necessary
    if (filenameLHE_ == "") {
      filenameLHE_ = generateFilenameStem() + ".lhe";
    }
  }

  if (vm.count("root")) {
    writeToROOT_ = true;
    // Generate default filename if necessary
    if (filenameROOT_ == "") {
      filenameROOT_ = generateFilenameStem() + ".root";
    }
  }
}


PythiaProgramOpts::~PythiaProgramOpts()
{
}


void PythiaProgramOpts::printProgramOptions() {
  cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
  cout << "PYTHIA PROGRAM OPTIONS" << endl;
  cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
  cout << "Reading settings from " << cardName_ << endl;
  if (writeToHEPMC_)
    cout << "Writing events to hepmc file " << filenameHEPMC_ << endl;
  if (writeToLHE_)
    cout << "Writing events to lhe file " << filenameLHE_ << endl;
  if (writeToROOT_)
    cout << "Saving histograms to ROOT file " << filenameROOT_ << endl;
  cout << "Generating " << nEvents_ << " events" << endl;
  cout << "Random seed: " << seed_ << endl;
  cout << "Mass of a1: " << mass_ << endl;
  cout << "CoM energy [TeV]: " << energy_ << endl;
  if (diMuFilter_)
    cout << "Using di-muon filter" << endl;
  cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
}


std::string PythiaProgramOpts::generateFilenameStem() {
  std::string channel = fs::path(cardName_).stem().string();
  return channel + "_ma1_" + lexical_cast<std::string>(mass_) + "_" +
    lexical_cast<std::string>(energy_) + "TeV_n" +
    lexical_cast<std::string>(nEvents_) + "_seed" + lexical_cast<std::string>(seed_);
}


void PythiaProgramOpts::printOptionError(po::error &e, std::string message) {
  cerr << message << ": " << e.what() << endl;
  cerr << desc_ << endl;
  cerr << "Exiting" << endl;
  exit(1);
}


bool PythiaProgramOpts::checkExtension(std::string filename, std::string ext) {
  boost::algorithm::to_lower(filename);
  return boost::algorithm::ends_with(filename, ext);
}
