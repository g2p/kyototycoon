/*************************************************************************************************
 * A lightweight database server
 *                                                               Copyright (C) 2009-2010 FAL Labs
 * This file is part of Kyoto Tycoon.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#include "cmdcommon.h"


// logger implementation
class Logger : public kt::RPCServer::Logger {
public:
  // constructor
  Logger() : strm_(NULL) {}
  // destructor
  ~Logger() {
    if (strm_) close();
  }
  // open the stream
  bool open(const char* path) {
    if (strm_) return false;
    if (path && *path != '\0' && std::strcmp(path, "-")) {
      std::ofstream* strm = new std::ofstream;
      strm->open(path, std::ios_base::out | std::ios_base::binary | std::ios_base::app);
      if (!strm) {
        delete strm;
        return false;
      }
      strm_ = strm;
    } else {
      strm_ = &std::cout;
    }
    return true;
  }
  // close the stream
  void close() {
    if (!strm_) return;
    if (strm_ != &std::cout) delete strm_;
    strm_ = NULL;
  }
private:
  // process a log message.
  void log(Kind kind, const char* message) {
    if (!strm_) return;
    char date[48];
    kt::datestrwww(kc::nan(), INT32_MAX, 6, date);
    const char* kstr = "MISC";
    switch (kind) {
      case kt::RPCServer::Logger::DEBUG: kstr = "DEBUG"; break;
      case kt::RPCServer::Logger::INFO: kstr = "INFO"; break;
      case kt::RPCServer::Logger::SYSTEM: kstr = "SYSTEM"; break;
      case kt::RPCServer::Logger::ERROR: kstr = "ERROR"; break;
    }
    *strm_ << date << ": [" << kstr << "]: " << message << "\n";
  }
  std::ostream* strm_;
};


// worker implementation
class Worker : public kt::RPCServer::Worker {
public:
  // constructor
  Worker() {
  }
  // destructor
  ~Worker() {
  }
private:
  // process each request of RPC.
  kt::RPCClient::ReturnValue process(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                                     const std::string& name,
                                     const std::map<std::string, std::string>& inmap,
                                     std::map<std::string, std::string>& outmap) {
    return kt::RPCClient::RVSUCCESS;
  }
  // process each request of the others.
  int32_t process(kt::HTTPServer* serv, kt::HTTPServer::Session* sess,
                  const std::string& path, kt::HTTPClient::Method method,
                  const std::map<std::string, std::string>& reqheads,
                  const std::string& reqbody,
                  std::map<std::string, std::string>& resheads,
                  std::string& resbody,
                  const std::map<std::string, std::string>& misc) {
    return 501;
  }
};


// global variables
const char* g_progname;                  // program name
int32_t g_procid;                        // process ID number
kt::RPCServer* g_serv;                   // running RPC server
bool g_restart;                          // restart flag


// function prototypes
int main(int argc, char** argv);
static void usage();
static void killserver(int signum);
static int32_t run(int argc, char** argv);
static int32_t proc(const std::vector<std::string>& dbpaths,
                    const char* host, int32_t port, double tout, int32_t thnum,
                    const char* logpath, uint32_t loglv, int32_t opts);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  g_procid = kc::getpid();
  kc::setstdiobin();
  kt::setkillsignalhandler(killserver);
  int32_t rv = run(argc, argv);
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: a lightweight database server of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s [-host str] [-port num] [-tout num] [-th num] [-log file] [-li|-ls|-le|-lz]"
          " [-tp] [db...]\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// kill the running server
static void killserver(int signum) {
  iprintf("%s: catched the signal %d\n", g_progname, signum);
  if (g_serv) {
    g_serv->stop();
    g_serv = NULL;
    if (signum == SIGHUP) g_restart = true;
  }
}


// parse arguments of the command
static int32_t run(int argc, char** argv) {
  std::vector<std::string> dbpaths;
  const char* host = NULL;
  int32_t port = DEFPORT;
  double tout = DEFTOUT;
  int32_t thnum = 1;
  const char* logpath = NULL;
  uint32_t loglv = UINT32_MAX;
  int32_t opts = 0;
  for (int32_t i = 1; i < argc; i++) {
    if (argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-log")) {
        if (++i >= argc) usage();
        logpath = argv[i];
      } else if (!std::strcmp(argv[i], "-li")) {
        loglv = kt::RPCServer::Logger::INFO | kt::RPCServer::Logger::SYSTEM |
          kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-ls")) {
        loglv = kt::RPCServer::Logger::SYSTEM | kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-le")) {
        loglv = kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-lz")) {
        loglv = 0;
      } else if (!std::strcmp(argv[i], "-tp")) {
        opts |= kt::TimedDB::TPERSIST;
      } else {
        usage();
      }
    } else {
      dbpaths.push_back(argv[i]);
    }
  }
  if (port < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  if (dbpaths.empty()) dbpaths.push_back("*");
  int32_t rv = proc(dbpaths, host, port, tout, thnum, logpath, loglv, opts);
  return rv;
}


// perform rpc command
static int32_t proc(const std::vector<std::string>& dbpaths,
                    const char* host, int32_t port, double tout, int32_t thnum,
                    const char* logpath, uint32_t loglv, int32_t opts) {
  std::string addr = "";
  if (host) {
    addr = kt::Socket::get_host_address(host);
    if (addr.empty()) {
      eprintf("%s: %s: unknown host\n", g_progname, host);
      return 1;
    }
  }
  std::string expr = kc::strprintf("%s:%d", addr.c_str(), port);


  bool err = false;
  g_restart = true;
  while (g_restart) {
    g_restart = false;
    kt::RPCServer serv;
    Logger logger;
    if (!logger.open(logpath)) {
      eprintf("%s: %s: could not open the log file\n", g_progname, logpath ? logpath : "-");
      err = true;
      break;
    }



    Worker worker;





    serv.set_network(expr, tout);
    serv.set_logger(&logger, loglv);
    serv.set_worker(&worker, thnum);
    g_serv = &serv;
    serv.log(kt::RPCServer::Logger::SYSTEM, "================ [START]: pid=%d", g_procid);
    if (serv.start()) {
      if (serv.finish()) err = true;
    } else {
      err = true;
      break;
    }
    serv.log(kt::RPCServer::Logger::SYSTEM, "================ [FINISH]: pid=%d", g_procid);
    logger.close();
  }

  return err ? 1 : 0;
}



// END OF FILE
