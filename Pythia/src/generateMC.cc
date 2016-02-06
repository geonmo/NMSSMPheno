#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <vector>
#include "Pythia8/Pythia.h"
// #include "fastjet/PseudoJet.hh"
// #include "fastjet/ClusterSequence.hh"
#include "Pythia8Plugins/HepMC2.h"

// ROOT headers
#include "TH1.h"
#include "TFile.h"
#include "TMath.h"
#include "TTree.h"

// BOOST headers
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

// Own headers
#include "PythiaProgramOpts.h"
#include "RootHistManager.h"

using std::cout;
using std::endl;
using boost::lexical_cast;
using namespace Pythia8;

namespace fs = boost::filesystem;

// Forward declare methods
std::vector<Particle*> getChildren(Event & event, Particle * p);
std::vector<Particle*> getAllDescendants(Event & event, Particle * p, bool finalStateOnly);
std::string getCurrentTime();
int gzip_file(std::string filename);
bool check_file_exists(std::string filename);

/**
 * @brief Main function for generating MC events
 */
int main(int argc, char *argv[]) {

  PythiaProgramOpts opts(argc, argv);
  opts.printProgramOptions();

  //---------------------------------------------------------------------------
  // SETUP PYTHIA
  //---------------------------------------------------------------------------
  Pythia pythia;

  std::string common_card = "input_cards/common_pp.cmnd";
  if (!check_file_exists(common_card))
    throw std::runtime_error("Common card \"" + common_card + "\" does not exist");
  pythia.readFile(common_card);

  if (!check_file_exists(opts.cardName()))
    throw std::runtime_error("Input card \"" + opts.cardName() + "\" does not exist");
  pythia.readFile(opts.cardName());

  pythia.readString("Beams:eCM = " + lexical_cast<std::string>(opts.energy() * 1000));

  pythia.readString("Main:numberOfEvents = " + lexical_cast<std::string>(opts.nEvents()));
  pythia.readString("Random:seed = " + lexical_cast<std::string>(opts.seed()));
  pythia.readString("36:m0 = " + lexical_cast<std::string>(opts.mass()));

  pythia.init();

  // Interface for conversion from Pythia8::Event to HepMC event.
  HepMC::Pythia8ToHepMC ToHepMC;
  HepMC::IO_GenEvent ascii_io(opts.filenameHEPMC(), std::ios::out);
  if (opts.writeToHEPMC()) {
    cout << "Writing HepMC to " << opts.filenameHEPMC() << endl;
  }

  // Create an LHAup object that can access relevant information in pythia for writing to LHE
  LHAupFromPYTHIA8 myLHA(&pythia.process, &pythia.info);
  if (opts.writeToLHE()) {
    cout << "Writing LHE to " << opts.filenameLHE() << endl;
    // Open a file on which LHEF events should be stored, and write header.
    myLHA.openLHEF(opts.filenameLHE());
    // Store initialization info in the LHAup object.
    myLHA.setInit();
    // Write out this initialization info on the file.
    myLHA.initLHEF();
  }

  // Text file to write progress - handy for monitoring during jobs
  ofstream progressFile;
  std::string stem = opts.generateFilenameStem();
  progressFile.open(stem + "_progress.txt");

  //---------------------------------------------------------------------------
  // SETUP ROOT TREES/HISTOGRAMS
  //---------------------------------------------------------------------------
  // need different Trees as we fill them at different rates, stop double counting
  // h1 variables
  TTree hTree("hVars", "hVars");
  float hPt(-1.), hEta(99.), hPhi(99.);
  float a1DPhi(99.), a1Dr(99.);
  hTree.Branch("hPt", &hPt, "hPt/Float_t");
  hTree.Branch("hEta", &hEta, "hEta/Float_t");
  hTree.Branch("hPhi", &hPhi, "hPhi/Float_t");
  hTree.Branch("a1DPhi", &a1DPhi, "a1DPhi/Float_t");
  hTree.Branch("a1Dr", &a1Dr, "a1Dr/Float_t");
  // a1 variables
  TTree a1Tree("a1Vars", "a1Vars");
  float a1Pt(-1.), a1Eta(99.), a1Phi(99.);
  float a1DecayDPhi(99.), a1DecayDr(99.);
  a1Tree.Branch("a1Pt", &a1Pt, "a1Pt/Float_t");
  a1Tree.Branch("a1Eta", &a1Eta, "a1Eta/Float_t");
  a1Tree.Branch("a1Phi", &a1Phi, "a1Phi/Float_t");
  a1Tree.Branch("a1DecayDPhi", &a1DecayDPhi, "a1DecayDPhi/Float_t");
  a1Tree.Branch("a1DecayDr", &a1DecayDr, "a1DecayDr/Float_t");
  // vars for a1 decay products (e.g. tau-tau)
  TTree a1DecayTree("a1DecayVars", "a1DecayVars");
  float a1DecayPt(-1.), a1DecayEta(99.), a1DecayPhi(99.);
  a1DecayTree.Branch("a1DecayPt", &a1DecayPt, "a1DecayPt/Float_t");
  a1DecayTree.Branch("a1DecayEta", &a1DecayEta, "a1DecayEta/Float_t");
  a1DecayTree.Branch("a1DecayPhi", &a1DecayPhi, "a1DecayPhi/Float_t");
  // vars for mu from a1 decay with cuts on 2 SS mu
  TTree a1DecayMuTree("a1DecayMuVars", "a1DecayMuVars");
  float a1DecayMuPt(-1.), a1DecayMuEta(99.), a1DecayMuPhi(99.);
  a1DecayMuTree.Branch("a1DecayMuPt", &a1DecayMuPt, "a1DecayMuPt/Float_t");
  a1DecayMuTree.Branch("a1DecayMuEta", &a1DecayMuEta, "a1DecayMuEta/Float_t");
  a1DecayMuTree.Branch("a1DecayMuPhi", &a1DecayMuPhi, "a1DecayMuPhi/Float_t");
  // vars for tau decays (all)
  TTree tauDecayTree("tauDecayVars", "tauDecayVars");
  float tauDecayPtRatio(-99.), tauDecayDr(99.);
  tauDecayTree.Branch("tauDecayPtRatio", &tauDecayPtRatio, "tauDecayPtRatio/Float_t");
  tauDecayTree.Branch("tauDecayDr", &tauDecayDr, "tauDecayDr/Float_t");
  // vars for tau decays (charged only)
  TTree tauDecayChargedTree("tauDecayChargedVars", "tauDecayChargedVars");
  float tauDecayChargedPtRatio(-99.), tauDecayChargedDr(99.);
  tauDecayChargedTree.Branch("tauDecayChargedPtRatio", &tauDecayChargedPtRatio, "tauDecayChargedPtRatio/Float_t");
  tauDecayChargedTree.Branch("tauDecayChargedDr", &tauDecayChargedDr, "tauDecayChargedDr/Float_t");

  //---------------------------------------------------------------------------
  // GENERATE EVENTS
  //---------------------------------------------------------------------------
  int progressFreq = 50;

  for (int iEvent = 0; iEvent < opts.nEvents();) {
    // output progress info
    if (iEvent % progressFreq == 0) {
      cout << "iEvent: " << iEvent << " - " << getCurrentTime() << endl;
      progressFile << "iEvent: " << iEvent << " - " << getCurrentTime() << endl;
    }

    // Generate event safely
    if (!pythia.next()) {
      break;
    }

    Event event = pythia.event;

    // Cut on number of muons
    if (opts.diMuFilter()) {
      int nMuons = 0;
      for (int i = 0; i < event.size(); ++i) {
        // check if final state and a muon
        if (event[i].idAbs() == 13 && event[i].status() > 0)
          nMuons++;
        if (nMuons == 2)
          break;
      }
      if (nMuons < 2)
        continue;
    }

    iEvent++;

    // Output to screen if wanted
    if (iEvent < 2 && opts.printEvent()) {
      pythia.info.list();
      pythia.event.list();
      pythia.process.list();
    }

    //-------------------------------------------------------------------------
    // STORE IN HEPMC/LHE
    //-------------------------------------------------------------------------
    // Construct new empty HepMC event and fill it.
    // Write the HepMC event to file. Done with it.
    if (opts.writeToHEPMC()) {
      HepMC::GenEvent* hepmcevt = new HepMC::GenEvent(HepMC::Units::GEV, HepMC::Units::MM);
      ToHepMC.fill_next_event(pythia, hepmcevt);
      ascii_io << hepmcevt;
      delete hepmcevt;
    }

    if (opts.writeToLHE()) {
      // Store event info in the LHAup object.
      myLHA.setEvent();
      // Write out this event info on the file.
      // With optional argument (verbose =) false the file is smaller.
      myLHA.eventLHEF();
    }

    //-------------------------------------------------------------------------
    // Analyse the event particles, fill histograms, etc.
    //-------------------------------------------------------------------------
    if (!opts.writeToROOT()) continue;

    bool donePlots = false;

    // find h decay products and look at separation
    for (int i = 0; i < event.size(); ++i) {
      if (donePlots) break; // skip the rest of the event listing, we're done

      // look at h1, its daughters (a1), and their daughters (tau, b, etc)
      if (event[i].idAbs() == 25 && event[i].status() == -62) {
        Particle & h1 = event[i];

        // plot h1 variables
        hPt = h1.pT();
        hEta = h1.eta();
        hPhi = h1.phi();

        int d1 = h1.daughter1();
        int d2 = h1.daughter2();
        a1Dr = REtaPhi(event[d1].p(), event[d2].p());
        a1DPhi = phi(event[d1].p(), event[d2].p());
        hTree.Fill();

        // now find all the h1 children (e.g. a1)
        // and plot child variables, and their decay products
        for (auto & a1Itr : getChildren(event, &h1)) {
          a1Pt = a1Itr->pT();
          a1Eta = a1Itr->eta();
          a1Phi = a1Itr->phi();

          // look at a1 daughter particles
          Vec4 daughter1Mom = event[a1Itr->daughter1()].p();
          if (a1Itr->daughter2() == 0 || a1Itr->daughter2() == a1Itr->daughter1()) {
            cout << "OH BUM" << endl;
          }
          Vec4 daughter2Mom = event[a1Itr->daughter2()].p();
          a1DecayDr = REtaPhi(daughter1Mom, daughter2Mom);
          a1DecayDPhi = phi(daughter1Mom, daughter2Mom);
          a1Tree.Fill();

          for (auto & dItr : getChildren(event, a1Itr)) {
            a1DecayPt = dItr->pT();
            a1DecayEta = dItr->eta();
            a1DecayPhi = dItr->phi();
            a1DecayTree.Fill();
          }

          // look at tau decay products
          for (auto & tauItr : getAllDescendants(event, a1Itr, false)) {
            // get taus that decay properly (not gamma radiation)
            if (tauItr->idAbs() == 15 && tauItr->daughterList().size() > 2) {
              for (auto & pItr : getAllDescendants(event, tauItr, true)) {
                // for all products
                tauDecayPtRatio = pItr->pT() / tauItr->pT();
                tauDecayDr = REtaPhi(pItr->p(), tauItr->p());
                tauDecayTree.Fill();
                // for charged products
                if (fabs(pItr->charge()) != 0) {
                  tauDecayChargedPtRatio = pItr->pT() / tauItr->pT();
                  tauDecayChargedDr = REtaPhi(pItr->p(), tauItr->p());
                  tauDecayChargedTree.Fill();
                }
              }
            }
          }
        }

        // analyze the muons in the tau decays. We want 2 SS muons.
        std::vector<Particle*> posMu;
        std::vector<Particle*> negMu;
        for (auto & itr : getAllDescendants(event, &h1, true)) {
          if (itr->idAbs() == 13) {
            if (itr->charge() > 0) {
              posMu.push_back(itr);
            } else {
              negMu.push_back(itr);
            }
          }
        }
        // get whichever charge collection has 2+ muons...
        std::vector<Particle*> a1mu;
        if (posMu.size() >= 2) {
          a1mu = posMu;
        } else if (negMu.size()) {
          a1mu = negMu;
        }

        // ...and plot some stuff
        for (auto & muItr : a1mu) {
          a1DecayMuPt = muItr->pT();
          a1DecayMuEta = muItr->eta();
          a1DecayMuPhi = muItr->phi();
          a1DecayMuTree.Fill();
        }

        donePlots = true;
      }
    }
  } // end of generating events loop

  progressFile.close();

  //---------------------------------------------------------------------------
  // PRINTOUT STATS & HISTOGRAMS
  //---------------------------------------------------------------------------
  pythia.stat();

  //---------------------------------------------------------------------------
  // WRITE ROOT HISTOGRAMS TO FILE & TIDY UP
  //---------------------------------------------------------------------------
  if (opts.writeToROOT()) {
      TFile * outFile = new TFile((opts.filenameROOT()).c_str(), "RECREATE");
      // histMan.write(outFile);
      hTree.Write("", TObject::kOverwrite);
      a1Tree.Write("", TObject::kOverwrite);
      a1DecayTree.Write("", TObject::kOverwrite);
      a1DecayMuTree.Write("", TObject::kOverwrite);
      tauDecayTree.Write("", TObject::kOverwrite);
      tauDecayChargedTree.Write("", TObject::kOverwrite);
      outFile->Close();
      delete outFile;
  }

  if (opts.writeToLHE()) {
    // Update the cross section info based on Monte Carlo integration during run.
    myLHA.updateSigma();
    // Write endtag. Overwrite initialization info with new cross sections.
    myLHA.closeLHEF(true);
  }

  if (opts.zip()) {
    // GZIP output to save space
    std::vector<std::string> filenames;
    if (opts.writeToLHE()) filenames.push_back(opts.filenameLHE());
    if (opts.writeToHEPMC()) filenames.push_back(opts.filenameHEPMC());

    for (const auto & fname : filenames) {
      int res = gzip_file(fname);
      if (res != 0) return res;
    }
  }

  return 0;
}


/**
 * @brief Return vector of Particle pointers to 'children' of the particle.
 *
 * @param event Pythia8::Event object holding particle listings
 * @param p Particle to get children.
 *
 * @return
 */
std::vector<Particle*> getChildren(Event & event, Particle * p) {
  std::vector<Particle*> children;
  for (int child = p->daughter1(); child <= p->daughter2(); ++child) {
    children.push_back(&event[child]);
  }
  return children;
}


/**
 * @brief Get all descendants from a given particle. Iterates through all the
 * generation of children, until they are all final state (status > 0).
 *
 * @param event [description]
 * @param p Particle to get descendants.
 * @param finalStateOnly If true, returns only descendants which are final state
 * (status > 0). Otherwise, returns all intermediate children as well.
 * @return Returns a vector of Particle*
 */
std::vector<Particle*> getAllDescendants(Event & event, Particle * p, bool finalStateOnly) {
  bool allFinalState = false;
  std::vector<Particle*> intermediates = {p}; // hold the intermediates
  std::vector<Particle*> descendants; // hold all the particles to be returned
  while (!allFinalState) {
    allFinalState = true;
    std::vector<Particle*> newIntermediates;
    for (auto & pItr : intermediates) {

      if (pItr->isFinal()) continue;

      auto kids = getChildren(event, pItr);
      for (auto & childItr : kids) {
        if (finalStateOnly) {
          if (childItr->isFinal()) {
            descendants.push_back(childItr);
          }
        } else {
          descendants.push_back(childItr);
        }
        allFinalState = allFinalState && childItr->isFinal();
      }
      newIntermediates.insert(newIntermediates.end(), kids.begin(), kids.end());
    }
    intermediates = newIntermediates;
    newIntermediates.clear(); // prob don't need this, but at least explicit
  }
  return descendants;
}


/**
 * @brief Get current time & date
 * @return std::string with time & date
 */
std::string getCurrentTime() {
  time_t now = time(0);
  char* dt = ctime(&now);
  std::string str1 = std::string(dt);
  boost::algorithm::trim(str1);
  return str1;
}


/**
 * @brief gzip file
 * @details The output will be <filename>.gz
 *
 * @param filename Name of the file to compress
 */
int gzip_file(std::string filename) {
  std::string cmd = "gzip -f " + filename;
  FILE * in = popen(cmd.c_str(), "r");
  if (!in) {
    return 1;
  }
  char buff[1024];
  while(fgets(buff, sizeof(buff), in) != NULL) {
    cout << buff;
  }
  pclose(in);
  return 0;
}

/**
 * @brief Check file exists
 *
 * @param filename File to check.
 * @return bool, true if file exists, false otherwise.
 */
bool check_file_exists(std::string filename) {
  return fs::exists(fs::path(filename));
}