/*************************************************************************************************
 * A persistent cache server
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


// global variables
const char* g_progname;                  // program name
int32_t g_procid;                        // process ID number
double g_starttime;                      // start time
kt::RPCServer* g_serv;                   // running RPC server
bool g_restart;                          // restart flag


// function prototypes
int main(int argc, char** argv);
static void usage();
static void killserver(int signum);
static int32_t run(int argc, char** argv);
static int32_t proc(const std::vector<std::string>& dbpaths,
                    const char* host, int32_t port, double tout, int32_t thnum,
                    const char* logpath, uint32_t logkinds, int32_t opts);


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
private:
  std::ostream* strm_;
};


// database logger implementation
class DBLogger : public kc::BasicDB::Logger {
public:
  // constructor
  DBLogger(::Logger* logger, uint32_t kinds) : logger_(logger), kinds_(kinds) {}
  // process a log message.
  void log(const char* file, int32_t line, const char* func,
           kc::BasicDB::Logger::Kind kind, const char* message) {
    kt::RPCServer::Logger::Kind rkind;
    switch (kind) {
      default: rkind = kt::RPCServer::Logger::DEBUG; break;
      case kc::BasicDB::Logger::INFO: rkind = kt::RPCServer::Logger::INFO; break;
      case kc::BasicDB::Logger::WARN: rkind = kt::RPCServer::Logger::SYSTEM; break;
      case kc::BasicDB::Logger::ERROR: rkind = kt::RPCServer::Logger::ERROR; break;
    }
    if (!(rkind & kinds_)) return;
    std::string lmsg;
    kc::strprintf(&lmsg, "[DB]: %s", message);
    logger_->log(rkind, lmsg.c_str());
  }
private:
  ::Logger* logger_;
  uint32_t kinds_;
};


// worker implementation
class Worker : public kt::RPCServer::Worker {
private:
  typedef kt::RPCClient::ReturnValue RV;
public:
  // constructor
  Worker(kt::TimedDB* dbs, int32_t dbnum, const std::map<std::string, int32_t>& dbmap) :
    dbs_(dbs), dbnum_(dbnum), dbmap_(dbmap) {}
  // destructor
  ~Worker() {}
private:
  // process each request of RPC.
  RV process(kt::RPCServer* serv, kt::RPCServer::Session* sess, const std::string& name,
             const std::map<std::string, std::string>& inmap,
             std::map<std::string, std::string>& outmap) {
    int32_t dbidx = 0;
    const char* rp = kt::strmapget(inmap, "DB");
    if (rp && *rp != '\0') {
      dbidx = -1;
      if (*rp >= '0' && *rp <= '9') {
        dbidx = kc::atoi(rp);
      } else {
        std::map<std::string, int32_t>::const_iterator it = dbmap_.find(rp);
        if (it != dbmap_.end()) dbidx = it->second;
      }
    }
    kt::TimedDB* db = dbidx >= 0 && dbidx < dbnum_ ? dbs_ + dbidx : NULL;
    RV rv;
    if (name == "echo") {
      rv = do_echo(serv, sess, db, inmap, outmap);
    } else if (name == "report") {
      rv = do_report(serv, sess, db, inmap, outmap);
    } else if (name == "status") {
      rv = do_status(serv, sess, db, inmap, outmap);
    } else if (name == "clear") {
      rv = do_clear(serv, sess, db, inmap, outmap);
    } else if (name == "set") {
      rv = do_set(serv, sess, db, inmap, outmap);
    } else if (name == "add") {
      rv = do_add(serv, sess, db, inmap, outmap);
    } else if (name == "replace") {
      rv = do_replace(serv, sess, db, inmap, outmap);
    } else if (name == "append") {
      rv = do_append(serv, sess, db, inmap, outmap);
    } else if (name == "increment") {
      rv = do_increment(serv, sess, db, inmap, outmap);
    } else if (name == "increment_double") {
      rv = do_increment_double(serv, sess, db, inmap, outmap);
    } else if (name == "cas") {
      rv = do_cas(serv, sess, db, inmap, outmap);
    } else if (name == "remove") {
      rv = do_remove(serv, sess, db, inmap, outmap);
    } else if (name == "get") {
      rv = do_get(serv, sess, db, inmap, outmap);
    } else {
      set_message(outmap, "ERROR", "no such procedure: %s", name.c_str());
      rv = kt::RPCClient::RVEINVALID;
    }
    return rv;
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
  // set the error message
  void set_message(std::map<std::string, std::string>& outmap, const char* key,
                   const char* format, ...) {
    std::string message;
    va_list ap;
    va_start(ap, format);
    kc::vstrprintf(&message, format, ap);
    va_end(ap);
    outmap[key] = message;
  }
  // set the database error message
  void set_db_error(std::map<std::string, std::string>& outmap, const kc::BasicDB::Error& e) {
    set_message(outmap, "ERROR", "DB: %d: %s: %s", e.code(), e.name(), e.message());
  }
  // process the echo procedure
  RV do_echo(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
             const std::map<std::string, std::string>& inmap,
             std::map<std::string, std::string>& outmap) {
    outmap.insert(inmap.begin(), inmap.end());
    return kt::RPCClient::RVSUCCESS;
  }
  // process the report procedure
  RV do_report(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
               const std::map<std::string, std::string>& inmap,
               std::map<std::string, std::string>& outmap) {
    set_message(outmap, "version", "%s (%d.%d)", kt::VERSION, kt::LIBVER, kt::LIBREV);
    set_message(outmap, "kc_version", "%s (%d.%d)", kc::VERSION, kc::LIBVER, kc::LIBREV);
    set_message(outmap, "os", "%s", kc::SYSNAME);
    set_message(outmap, "pid", "%d", g_procid);
    set_message(outmap, "time", "%.6f", kc::time() - g_starttime);
    int64_t totalcount = 0;
    int64_t totalsize = 0;
    for (int32_t i = 0; i < dbnum_; i++) {
      int64_t count = dbs_[i].count();
      int64_t size = dbs_[i].size();
      std::string key;
      kc::strprintf(&key, "db_%d", i);
      set_message(outmap, key.c_str(), "count=%lld size=%lld path=%s",
                  (long long)count, (long long)size, dbs_[i].path().c_str());
      totalcount += count;
      totalsize += size;
    }
    set_message(outmap, "count", "%lld", (long long)totalcount);
    set_message(outmap, "size", "%lld", (long long)totalsize);
    std::map<std::string, std::string> sysinfo;
    kc::getsysinfo(&sysinfo);
    std::map<std::string, std::string>::iterator it = sysinfo.begin();
    std::map<std::string, std::string>::iterator itend = sysinfo.end();
    while (it != itend) {
      std::string key;
      kc::strprintf(&key, "sys_%s", it->first.c_str());
      set_message(outmap, key.c_str(), it->second.c_str());
      it++;
    }
    return kt::RPCClient::RVSUCCESS;
  }
  // process the status procedure
  RV do_status(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
               const std::map<std::string, std::string>& inmap,
               std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    RV rv;
    std::map<std::string, std::string> status;
    if (db->status(&status)) {
      rv = kt::RPCClient::RVSUCCESS;
      outmap.insert(status.begin(), status.end());
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the clear procedure
  RV do_clear(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
              const std::map<std::string, std::string>& inmap,
              std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    RV rv;
    if (db->clear()) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the set procedure
  RV do_set(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
            const std::map<std::string, std::string>& inmap,
            std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    size_t vsiz;
    const char* vbuf = kt::strmapget(inmap, "value", &vsiz);
    if (!kbuf || !vbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : -1;
    if (xt < 1) xt = INT64_MAX;
    RV rv;
    if (db->set(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the add procedure
  RV do_add(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
            const std::map<std::string, std::string>& inmap,
            std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    size_t vsiz;
    const char* vbuf = kt::strmapget(inmap, "value", &vsiz);
    if (!kbuf || !vbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : -1;
    if (xt < 1) xt = INT64_MAX;
    RV rv;
    if (db->add(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = e == kc::BasicDB::Error::DUPREC ?
        kt::RPCClient::RVELOGIC : kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the replace procedure
  RV do_replace(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
                const std::map<std::string, std::string>& inmap,
                std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    size_t vsiz;
    const char* vbuf = kt::strmapget(inmap, "value", &vsiz);
    if (!kbuf || !vbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : -1;
    if (xt < 1) xt = INT64_MAX;
    RV rv;
    if (db->replace(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = e == kc::BasicDB::Error::NOREC ?
        kt::RPCClient::RVELOGIC : kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the append procedure
  RV do_append(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
               const std::map<std::string, std::string>& inmap,
               std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    size_t vsiz;
    const char* vbuf = kt::strmapget(inmap, "value", &vsiz);
    if (!kbuf || !vbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : -1;
    if (xt < 1) xt = INT64_MAX;
    RV rv;
    if (db->append(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the increment procedure
  RV do_increment(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
                  const std::map<std::string, std::string>& inmap,
                  std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    const char* nstr = kt::strmapget(inmap, "num");
    if (!kbuf || !nstr) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    int64_t num = kc::atoi(nstr);
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : -1;
    if (xt < 1) xt = INT64_MAX;
    RV rv;
    num = db->increment(kbuf, ksiz, num, xt);
    if (num != INT64_MIN) {
      rv = kt::RPCClient::RVSUCCESS;
      set_message(outmap, "num", "%lld", (long long)num);
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = e == kc::BasicDB::Error::LOGIC ?
        kt::RPCClient::RVELOGIC : kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the increment_double procedure
  RV do_increment_double(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
                         const std::map<std::string, std::string>& inmap,
                         std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    const char* nstr = kt::strmapget(inmap, "num");
    if (!kbuf || !nstr) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    double num = kc::atof(nstr);
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : -1;
    if (xt < 1) xt = INT64_MAX;
    RV rv;
    num = db->increment_double(kbuf, ksiz, num, xt);
    if (!kc::chknan(num)) {
      rv = kt::RPCClient::RVSUCCESS;
      set_message(outmap, "num", "%f", num);
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = e == kc::BasicDB::Error::LOGIC ?
        kt::RPCClient::RVELOGIC : kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the cas procedure
  RV do_cas(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
            const std::map<std::string, std::string>& inmap,
            std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    if (!kbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ovsiz;
    const char* ovbuf = kt::strmapget(inmap, "oval", &ovsiz);
    size_t nvsiz;
    const char* nvbuf = kt::strmapget(inmap, "nval", &nvsiz);
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : -1;
    if (xt < 1) xt = INT64_MAX;
    RV rv;
    if (db->cas(kbuf, ksiz, ovbuf, ovsiz, nvbuf, nvsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = e == kc::BasicDB::Error::LOGIC ?
        kt::RPCClient::RVELOGIC : kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the remove procedure
  RV do_remove(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
               const std::map<std::string, std::string>& inmap,
               std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    if (!kbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    RV rv;
    if (db->remove(kbuf, ksiz)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = e == kc::BasicDB::Error::NOREC ?
        kt::RPCClient::RVELOGIC : kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the get procedure
  RV do_get(kt::RPCServer* serv, kt::RPCServer::Session* sess, kt::TimedDB* db,
            const std::map<std::string, std::string>& inmap,
            std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    if (!kbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    RV rv;
    size_t vsiz;
    int64_t xt;
    const char* vbuf = db->get(kbuf, ksiz, &vsiz, &xt);
    if (vbuf) {
      outmap["value"] = std::string(vbuf, vsiz);
      if (xt < kt::TimedDB::XTMAX) kc::strprintf(&outmap["xt"], "%lld", (long long)xt);
      delete[] vbuf;
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      kc::BasicDB::Error e = db->error();
      set_db_error(outmap, e);
      rv = e == kc::BasicDB::Error::NOREC ?
        kt::RPCClient::RVELOGIC : kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  kt::TimedDB* const dbs_;
  const int32_t dbnum_;
  const std::map<std::string, int32_t>& dbmap_;
};


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  g_procid = kc::getpid();
  g_starttime = kc::time();
  kc::setstdiobin();
  kt::setkillsignalhandler(killserver);
  if (argc > 1 && !std::strcmp(argv[1], "--version")) {
    printversion();
    return 0;
  }
  int32_t rv = run(argc, argv);
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: Kyoto Tycoon: a persistent cache server\n", g_progname);
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
  bool argbrk = false;
  std::vector<std::string> dbpaths;
  const char* host = NULL;
  int32_t port = kt::DEFPORT;
  double tout = DEFTOUT;
  int32_t thnum = DEFTHNUM;
  const char* logpath = NULL;
  uint32_t logkinds = UINT32_MAX;
  int32_t opts = 0;
  for (int32_t i = 1; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
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
        logkinds = kt::RPCServer::Logger::INFO | kt::RPCServer::Logger::SYSTEM |
          kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-ls")) {
        logkinds = kt::RPCServer::Logger::SYSTEM | kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-le")) {
        logkinds = kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-lz")) {
        logkinds = 0;
      } else if (!std::strcmp(argv[i], "-tp")) {
        opts |= kt::TimedDB::TPERSIST;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      dbpaths.push_back(argv[i]);
    }
  }
  if (port < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  if (dbpaths.empty()) dbpaths.push_back("*");
  int32_t rv = proc(dbpaths, host, port, tout, thnum, logpath, logkinds, opts);
  return rv;
}


// perform rpc command
static int32_t proc(const std::vector<std::string>& dbpaths,
                    const char* host, int32_t port, double tout, int32_t thnum,
                    const char* logpath, uint32_t logkinds, int32_t opts) {
  kt::RPCServer serv;
  Logger logger;
  if (!logger.open(logpath)) {
    eprintf("%s: %s: could not open the log file\n", g_progname, logpath ? logpath : "-");
    return 1;
  }
  serv.set_logger(&logger, logkinds);
  serv.log(kt::RPCServer::Logger::SYSTEM, "================ [START]: pid=%d", g_procid);
  std::string addr = "";
  if (host) {
    addr = kt::Socket::get_host_address(host);
    if (addr.empty()) {
      serv.log(kt::RPCServer::Logger::ERROR, "%s: unknown host\n", g_progname, host);
      return 1;
    }
  }
  std::string expr = kc::strprintf("%s:%d", addr.c_str(), port);
  serv.set_network(expr, tout);
  int32_t dbnum = dbpaths.size();
  kt::TimedDB* dbs = new kt::TimedDB[dbnum];
  DBLogger dblogger(&logger, logkinds);
  std::map<std::string, int32_t> dbmap;
  for (int32_t i = 0; i < dbnum; i++) {
    const std::string& dbpath = dbpaths[i];
    serv.log(kt::RPCServer::Logger::SYSTEM, "opening a database: path=%s", dbpath.c_str());
    if (logkinds != 0)
      dbs[i].tune_logger(&dblogger, kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR);
    if (opts > 0) dbs[i].tune_options(opts);
    if (!dbs[i].open(dbpath)) {
      kc::BasicDB::Error e = dbs[i].error();
      serv.log(kt::RPCServer::Logger::ERROR, "%s: could not open a database file: %s: %s\n",
               dbpath.c_str(), e.name(), e.message());
      delete[] dbs;
      return 1;
    }
    std::string path = dbs[i].path();
    const char* rp = path.c_str();
    const char* pv = std::strrchr(rp, kc::File::PATHCHR);
    if (pv) rp = pv + 1;
    dbmap[rp] = i;
  }
  Worker worker(dbs, dbnum, dbmap);
  serv.set_worker(&worker, thnum);
  bool err = false;
  g_restart = true;
  while (g_restart) {
    g_restart = false;
    g_serv = &serv;
    if (serv.start()) {
      if (serv.finish()) err = true;
    } else {
      err = true;
      break;
    }
    logger.close();
    if (!logger.open(logpath)) {
      eprintf("%s: %s: could not open the log file\n", g_progname, logpath ? logpath : "-");
      err = true;
      break;
    }
  }
  delete[] dbs;
  serv.log(kt::RPCServer::Logger::SYSTEM, "================ [FINISH]: pid=%d", g_procid);
  return err ? 1 : 0;
}



// END OF FILE
