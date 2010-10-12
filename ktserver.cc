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
bool g_daemon;                           // daemon flag
kt::RPCServer* g_serv;                   // running RPC server
bool g_restart;                          // restart flag


// function prototypes
int main(int argc, char** argv);
static void usage();
static void killserver(int signum);
static int32_t run(int argc, char** argv);
static int32_t proc(const std::vector<std::string>& dbpaths,
                    const char* host, int32_t port, double tout, int32_t thnum,
                    const char* logpath, uint32_t logkinds, int32_t omode, int32_t opts,
                    bool dmn, const char* pidpath, const char* cmdpath, const char* scrpath);


// logger implementation
class Logger : public kt::RPCServer::Logger {
public:
  // constructor
  Logger() : strm_(NULL), lock_() {}
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
      if (!*strm) {
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
    lock_.lock();
    *strm_ << date << ": [" << kstr << "]: " << message << "\n";
    strm_->flush();
    lock_.unlock();
  }
private:
  std::ostream* strm_;
  kc::Mutex lock_;
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
  class SLS;
public:
  // constructor
  Worker(int32_t thnum, kt::TimedDB* dbs, int32_t dbnum,
         const std::map<std::string, int32_t>& dbmap, int32_t omode,
         const char* cmdpath, ScriptProcessor* scrprocs) :
    thnum_(thnum), dbs_(dbs), dbnum_(dbnum), dbmap_(dbmap), omode_(omode),
    cmdpath_(cmdpath), scrprocs_(scrprocs), idlecnt_(0) {}
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
    int64_t curid = -1;
    rp = kt::strmapget(inmap, "CUR");
    if (rp && *rp >= '0' && *rp <= '9') curid = kc::atoi(rp);
    kt::TimedDB::Cursor* cur = NULL;
    if (curid >= 0) {
      SLS* sls = SLS::create(sess);
      std::map<int64_t, kt::TimedDB::Cursor*>::iterator it = sls->curs_.find(curid);
      if (it == sls->curs_.end()) {
        if (db) {
          cur = db->cursor();
          sls->curs_[curid] = cur;
        }
      } else {
        cur = it->second;
        if (name == "cur_delete") {
          sls->curs_.erase(curid);
          delete cur;
          return kt::RPCClient::RVSUCCESS;
        }
      }
    }
    RV rv;
    if (name == "echo") {
      rv = do_echo(serv, sess, inmap, outmap);
    } else if (name == "report") {
      rv = do_report(serv, sess, inmap, outmap);
    } else if (name == "play_script") {
      rv = do_play_script(serv, sess, inmap, outmap);
    } else if (name == "status") {
      rv = do_status(serv, sess, db, inmap, outmap);
    } else if (name == "clear") {
      rv = do_clear(serv, sess, db, inmap, outmap);
    } else if (name == "synchronize") {
      rv = do_synchronize(serv, sess, db, inmap, outmap);
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
    } else if (name == "set_bulk") {
      rv = do_set_bulk(serv, sess, db, inmap, outmap);
    } else if (name == "remove_bulk") {
      rv = do_remove_bulk(serv, sess, db, inmap, outmap);
    } else if (name == "get_bulk") {
      rv = do_get_bulk(serv, sess, db, inmap, outmap);
    } else if (name == "vacuum") {
      rv = do_vacuum(serv, sess, db, inmap, outmap);
    } else if (name == "cur_jump") {
      rv = do_cur_jump(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_jump_back") {
      rv = do_cur_jump_back(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_step") {
      rv = do_cur_step(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_step_back") {
      rv = do_cur_step_back(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_set_value") {
      rv = do_cur_set_value(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_remove") {
      rv = do_cur_remove(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_get_key") {
      rv = do_cur_get_key(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_get_value") {
      rv = do_cur_get_value(serv, sess, cur, inmap, outmap);
    } else if (name == "cur_get") {
      rv = do_cur_get(serv, sess, cur, inmap, outmap);
    } else {
      set_message(outmap, "ERROR", "not implemented: %s", name.c_str());
      rv = kt::RPCClient::RVENOIMPL;
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
    const char* pstr = path.c_str();
    if (*pstr == '/') pstr++;
    int32_t dbidx = 0;
    const char* rp = std::strchr(pstr, '/');
    if (rp) {
      std::string dbexpr(pstr, rp - pstr);
      pstr = rp + 1;
      if (*pstr == '/') pstr++;
      size_t desiz;
      char* destr = kc::urldecode(dbexpr.c_str(), &desiz);
      if (*destr != '\0') {
        dbidx = -1;
        if (*destr >= '0' && *destr <= '9') {
          dbidx = kc::atoi(destr);
        } else {
          std::map<std::string, int32_t>::const_iterator it = dbmap_.find(destr);
          if (it != dbmap_.end()) dbidx = it->second;
        }
      }
      delete[] destr;
    }
    if (dbidx < 0 || dbidx >= dbnum_) {
      resbody.append("no such database\n");
      return 400;
    }
    kt::TimedDB* db = dbs_ + dbidx;
    size_t ksiz;
    char* kbuf = kc::urldecode(pstr, &ksiz);
    int32_t code;
    switch (method) {
      case kt::HTTPClient::MGET: {
        code = do_rest_get(serv, sess, db, kbuf, ksiz,
                           reqheads, reqbody, resheads, resbody, misc);
        break;
      }
      case kt::HTTPClient::MHEAD: {
        code = do_rest_head(serv, sess, db, kbuf, ksiz,
                            reqheads, reqbody, resheads, resbody, misc);
        break;
      }
      case kt::HTTPClient::MPUT: {
        code = do_rest_put(serv, sess, db, kbuf, ksiz,
                           reqheads, reqbody, resheads, resbody, misc);
        break;
      }
      case kt::HTTPClient::MDELETE: {
        code = do_rest_delete(serv, sess, db, kbuf, ksiz,
                              reqheads, reqbody, resheads, resbody, misc);
        break;
      }
      default: {
        code = 501;
        break;
      }
    }
    delete[] kbuf;
    return code;
  }
  // process each idle ivent
  void process_idle(kt::RPCServer* serv) {
    if (!(omode_ & kc::BasicDB::OWRITER)) return;
    int32_t dbidx = idlecnt_.add(1) % dbnum_;
    kt::TimedDB* db = dbs_ + dbidx;
    if (!db->vacuum(4)) {
      const kc::BasicDB::Error& e = db->error();
      log_db_error(serv, e);
    }
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
  // log the database error message
  void log_db_error(kt::RPCServer* serv, const kc::BasicDB::Error& e) {
    log_db_error(serv->reveal_core(), e);
  }
  // log the database error message
  void log_db_error(kt::HTTPServer* serv, const kc::BasicDB::Error& e) {
    serv->log(kt::RPCServer::Logger::ERROR, "database error: %d: %s: %s",
               e.code(), e.name(), e.message());
  }
  // process the echo procedure
  RV do_echo(kt::RPCServer* serv, kt::RPCServer::Session* sess,
             const std::map<std::string, std::string>& inmap,
             std::map<std::string, std::string>& outmap) {
    outmap.insert(inmap.begin(), inmap.end());
    return kt::RPCClient::RVSUCCESS;
  }
  // process the report procedure
  RV do_report(kt::RPCServer* serv, kt::RPCServer::Session* sess,
               const std::map<std::string, std::string>& inmap,
               std::map<std::string, std::string>& outmap) {
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
    set_message(outmap, "db_total_count", "%lld", (long long)totalcount);
    set_message(outmap, "db_total_size", "%lld", (long long)totalsize);
    kt::ThreadedServer* thserv = serv->reveal_core()->reveal_core();
    set_message(outmap, "serv_conn", "%lld", (long long)thserv->connection_count());
    set_message(outmap, "serv_task", "%lld", (long long)thserv->task_count());
    set_message(outmap, "conf_kt_version", "%s (%d.%d)", kt::VERSION, kt::LIBVER, kt::LIBREV);
    set_message(outmap, "conf_kc_version", "%s (%d.%d)", kc::VERSION, kc::LIBVER, kc::LIBREV);
    set_message(outmap, "conf_os_name", "%s", kc::SYSNAME);
    set_message(outmap, "sys_proc_id", "%d", g_procid);
    set_message(outmap, "sys_time", "%.6f", kc::time() - g_starttime);
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
  // process the play_script procedure
  RV do_play_script(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                    const std::map<std::string, std::string>& inmap,
                    std::map<std::string, std::string>& outmap) {
    if (!scrprocs_) {
      set_message(outmap, "ERROR", "the scripting extention is disabled");
      return kt::RPCClient::RVELOGIC;
    }
    int32_t thid = sess->thread_id();
    if (thid >= thnum_) {
      set_message(outmap, "ERROR", "the thread ID is invalid");
      return kt::RPCClient::RVEINTERNAL;
    }
    const char* nstr = kt::strmapget(inmap, "name");
    if (!nstr || *nstr == '\0' || !kt::strisalnum(nstr)) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    ScriptProcessor* scrproc = scrprocs_ + thid;
    std::map<std::string, std::string> scrinmap;
    std::map<std::string, std::string>::const_iterator it = inmap.begin();
    std::map<std::string, std::string>::const_iterator itend = inmap.end();
    while (it != itend) {
      const char* kbuf = it->first.data();
      size_t ksiz = it->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        std::string key(kbuf + 1, ksiz - 1);
        scrinmap[key] = it->second;
      }
      it++;
    }
    std::map<std::string, std::string> scroutmap;
    RV rv = scrproc->call(nstr, scrinmap, scroutmap);
    if (rv == kt::RPCClient::RVSUCCESS) {
      it = scroutmap.begin();
      itend = scroutmap.end();
      while (it != itend) {
        std::string key = "_";
        key.append(it->first);
        outmap[key] = it->second;
        it++;
      }
    } else {
      set_message(outmap, "ERROR", "a scripting procedure failed");
    }
    return rv;
  }
  // process the status procedure
  RV do_status(kt::RPCServer* serv, kt::RPCServer::Session* sess,
               kt::TimedDB* db,
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
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      log_db_error(serv, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the clear procedure
  RV do_clear(kt::RPCServer* serv, kt::RPCServer::Session* sess,
              kt::TimedDB* db,
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
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      log_db_error(serv, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the synchronize procedure
  RV do_synchronize(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                    kt::TimedDB* db,
                    const std::map<std::string, std::string>& inmap,
                    std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "hard");
    bool hard = rp ? true : false;
    rp = kt::strmapget(inmap, "command");
    std::string command = rp ? rp : "";
    class Visitor : public kc::BasicDB::FileProcessor {
    public:
      Visitor(kt::RPCServer* serv, Worker* worker, const std::string& command) :
        serv_(serv), worker_(worker), command_(command) {}
    private:
      bool process(const std::string& path, int64_t count, int64_t size) {
        if (command_.size() < 1) return true;
        const char* cmd = command_.c_str();
        if (std::strchr(cmd, kc::File::PATHCHR) || !std::strcmp(cmd, kc::File::CDIRSTR) ||
            !std::strcmp(cmd, kc::File::PDIRSTR)) {
          serv_->log(kt::RPCServer::Logger::ERROR, "invalid command name: %s", cmd);
          return false;
        }
        std::string cmdpath;
        kc::strprintf(&cmdpath, "%s%c%s", worker_->cmdpath_, kc::File::PATHCHR, cmd);
        std::vector<std::string> args;
        args.push_back(cmdpath);
        args.push_back(path);
        serv_->log(kt::RPCServer::Logger::SYSTEM, "executing: %s \"%s\"", cmd, path.c_str());
        if (kt::executecommand(args) != 0) {
          serv_->log(kt::RPCServer::Logger::ERROR, "execution failed: %s \"%s\"",
                     cmd, path.c_str());
          return false;
        }
        return true;
      }
      kt::RPCServer* serv_;
      Worker* worker_;
      std::string command_;
    };
    Visitor visitor(serv, this, command);
    RV rv;
    if (db->synchronize(hard, &visitor)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      log_db_error(serv, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the set procedure
  RV do_set(kt::RPCServer* serv, kt::RPCServer::Session* sess,
            kt::TimedDB* db,
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
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    if (db->set(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      log_db_error(serv, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the add procedure
  RV do_add(kt::RPCServer* serv, kt::RPCServer::Session* sess,
            kt::TimedDB* db,
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
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    if (db->add(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::DUPREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the replace procedure
  RV do_replace(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                kt::TimedDB* db,
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
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    if (db->replace(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the append procedure
  RV do_append(kt::RPCServer* serv, kt::RPCServer::Session* sess,
               kt::TimedDB* db,
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
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    if (db->append(kbuf, ksiz, vbuf, vsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      log_db_error(serv, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the increment procedure
  RV do_increment(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                  kt::TimedDB* db,
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
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    num = db->increment(kbuf, ksiz, num, xt);
    if (num != INT64_MIN) {
      rv = kt::RPCClient::RVSUCCESS;
      set_message(outmap, "num", "%lld", (long long)num);
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::LOGIC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the increment_double procedure
  RV do_increment_double(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                         kt::TimedDB* db,
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
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    num = db->increment_double(kbuf, ksiz, num, xt);
    if (!kc::chknan(num)) {
      rv = kt::RPCClient::RVSUCCESS;
      set_message(outmap, "num", "%f", num);
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::LOGIC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the cas procedure
  RV do_cas(kt::RPCServer* serv, kt::RPCServer::Session* sess,
            kt::TimedDB* db,
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
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    if (db->cas(kbuf, ksiz, ovbuf, ovsiz, nvbuf, nvsiz, xt)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::LOGIC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the remove procedure
  RV do_remove(kt::RPCServer* serv, kt::RPCServer::Session* sess,
               kt::TimedDB* db,
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
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the get procedure
  RV do_get(kt::RPCServer* serv, kt::RPCServer::Session* sess,
            kt::TimedDB* db,
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
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the set_bulk procedure
  RV do_set_bulk(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                 kt::TimedDB* db,
                 const std::map<std::string, std::string>& inmap,
                 std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    bool err = false;
    int64_t num = 0;
    std::map<std::string, std::string>::const_iterator it = inmap.begin();
    std::map<std::string, std::string>::const_iterator itend = inmap.end();
    while (it != itend) {
      const char* kbuf = it->first.data();
      size_t ksiz = it->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        if (db->set(kbuf + 1, ksiz - 1, it->second.data(), it->second.size(), xt)) {
          num++;
        } else {
          const kc::BasicDB::Error& e = db->error();
          set_db_error(outmap, e);
          log_db_error(serv, e);
          err = true;
        }
      }
      it++;
    }
    if (!err) kc::strprintf(&outmap["num"], "%lld", (long long)num);
    return err ? kt::RPCClient::RVEINTERNAL : kt::RPCClient::RVSUCCESS;
  }
  // process the remove_bulk procedure
  RV do_remove_bulk(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                    kt::TimedDB* db,
                    const std::map<std::string, std::string>& inmap,
                    std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    bool err = false;
    int64_t num = 0;
    std::map<std::string, std::string>::const_iterator it = inmap.begin();
    std::map<std::string, std::string>::const_iterator itend = inmap.end();
    while (it != itend) {
      const char* kbuf = it->first.data();
      size_t ksiz = it->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        if (db->remove(kbuf + 1, ksiz - 1)) {
          num++;
        } else {
          const kc::BasicDB::Error& e = db->error();
          if (e != kc::BasicDB::Error::NOREC) {
            set_db_error(outmap, e);
            log_db_error(serv, e);
            err = true;
          }
        }
      }
      it++;
    }
    if (!err) kc::strprintf(&outmap["num"], "%lld", (long long)num);
    return err ? kt::RPCClient::RVEINTERNAL : kt::RPCClient::RVSUCCESS;
  }
  // process the get_bulk procedure
  RV do_get_bulk(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                 kt::TimedDB* db,
                 const std::map<std::string, std::string>& inmap,
                 std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    bool err = false;
    int64_t num = 0;
    std::map<std::string, std::string>::const_iterator it = inmap.begin();
    std::map<std::string, std::string>::const_iterator itend = inmap.end();
    while (it != itend) {
      const char* kbuf = it->first.data();
      size_t ksiz = it->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        size_t vsiz;
        char* vbuf = db->get(kbuf + 1, ksiz - 1, &vsiz);
        if (vbuf) {
          outmap[it->first] = std::string(vbuf, vsiz);
          delete[] vbuf;
          num++;
        } else {
          const kc::BasicDB::Error& e = db->error();
          if (e != kc::BasicDB::Error::NOREC) {
            set_db_error(outmap, e);
            log_db_error(serv, e);
            err = true;
          }
        }
      }
      it++;
    }
    if (!err) kc::strprintf(&outmap["num"], "%lld", (long long)num);
    return err ? kt::RPCClient::RVEINTERNAL : kt::RPCClient::RVSUCCESS;
  }
  // process the vacuum procedure
  RV do_vacuum(kt::RPCServer* serv, kt::RPCServer::Session* sess,
               kt::TimedDB* db,
               const std::map<std::string, std::string>& inmap,
               std::map<std::string, std::string>& outmap) {
    if (!db) {
      set_message(outmap, "ERROR", "no such database");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "step");
    int64_t step = rp ? kc::atoi(rp) : 0;
    RV rv;
    if (db->vacuum(step)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = db->error();
      set_db_error(outmap, e);
      log_db_error(serv, e);
      rv = kt::RPCClient::RVEINTERNAL;
    }
    return rv;
  }
  // process the cur_jump procedure
  RV do_cur_jump(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                 kt::TimedDB::Cursor* cur,
                 const std::map<std::string, std::string>& inmap,
                 std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    RV rv;
    if (kbuf) {
      if (cur->jump(kbuf, ksiz)) {
        rv = kt::RPCClient::RVSUCCESS;
      } else {
        const kc::BasicDB::Error& e = cur->error();
        set_db_error(outmap, e);
        if (e == kc::BasicDB::Error::NOREC) {
          rv = kt::RPCClient::RVELOGIC;
        } else {
          log_db_error(serv, e);
          rv = kt::RPCClient::RVEINTERNAL;
        }
      }
    } else {
      if (cur->jump()) {
        rv = kt::RPCClient::RVSUCCESS;
      } else {
        const kc::BasicDB::Error& e = cur->error();
        set_db_error(outmap, e);
        if (e == kc::BasicDB::Error::NOREC) {
          rv = kt::RPCClient::RVELOGIC;
        } else {
          log_db_error(serv, e);
          rv = kt::RPCClient::RVEINTERNAL;
        }
      }
    }
    return rv;
  }
  // process the cur_jump_back procedure
  RV do_cur_jump_back(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                      kt::TimedDB::Cursor* cur,
                      const std::map<std::string, std::string>& inmap,
                      std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    size_t ksiz;
    const char* kbuf = kt::strmapget(inmap, "key", &ksiz);
    RV rv;
    if (kbuf) {
      if (cur->jump_back(kbuf, ksiz)) {
        rv = kt::RPCClient::RVSUCCESS;
      } else {
        const kc::BasicDB::Error& e = cur->error();
        set_db_error(outmap, e);
        switch (e.code()) {
          case kc::BasicDB::Error::NOIMPL: {
            rv = kt::RPCClient::RVENOIMPL;
            break;
          }
          case kc::BasicDB::Error::NOREC: {
            rv = kt::RPCClient::RVELOGIC;
            break;
          }
          default: {
            log_db_error(serv, e);
            rv = kt::RPCClient::RVEINTERNAL;
            break;
          }
        }
      }
    } else {
      if (cur->jump_back()) {
        rv = kt::RPCClient::RVSUCCESS;
      } else {
        const kc::BasicDB::Error& e = cur->error();
        set_db_error(outmap, e);
        switch (e.code()) {
          case kc::BasicDB::Error::NOIMPL: {
            rv = kt::RPCClient::RVENOIMPL;
            break;
          }
          case kc::BasicDB::Error::NOREC: {
            rv = kt::RPCClient::RVELOGIC;
            break;
          }
          default: {
            log_db_error(serv, e);
            rv = kt::RPCClient::RVEINTERNAL;
            break;
          }
        }
      }
    }
    return rv;
  }
  // process the cur_step procedure
  RV do_cur_step(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                 kt::TimedDB::Cursor* cur,
                 const std::map<std::string, std::string>& inmap,
                 std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    RV rv;
    if (cur->step()) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = cur->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the cur_step_back procedure
  RV do_cur_step_back(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                      kt::TimedDB::Cursor* cur,
                      const std::map<std::string, std::string>& inmap,
                      std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    RV rv;
    if (cur->step_back()) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = cur->error();
      set_db_error(outmap, e);
      switch (e.code()) {
        case kc::BasicDB::Error::NOIMPL: {
          rv = kt::RPCClient::RVENOIMPL;
          break;
        }
        case kc::BasicDB::Error::NOREC: {
          rv = kt::RPCClient::RVELOGIC;
          break;
        }
        default: {
          log_db_error(serv, e);
          rv = kt::RPCClient::RVEINTERNAL;
          break;
        }
      }
    }
    return rv;
  }
  // process the cur_set_value procedure
  RV do_cur_set_value(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                      kt::TimedDB::Cursor* cur,
                      const std::map<std::string, std::string>& inmap,
                      std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    size_t vsiz;
    const char* vbuf = kt::strmapget(inmap, "value", &vsiz);
    if (!vbuf) {
      set_message(outmap, "ERROR", "invalid parameters");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "step");
    bool step = rp ? true : false;
    rp = kt::strmapget(inmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    RV rv;
    if (cur->set_value(vbuf, vsiz, xt, step)) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = cur->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the remove procedure
  RV do_cur_remove(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                   kt::TimedDB::Cursor* cur,
                   const std::map<std::string, std::string>& inmap,
                   std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    RV rv;
    if (cur->remove()) {
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = cur->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the cur_get_key procedure
  RV do_cur_get_key(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                    kt::TimedDB::Cursor* cur,
                    const std::map<std::string, std::string>& inmap,
                    std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "step");
    bool step = rp ? true : false;
    RV rv;
    size_t ksiz;
    char* kbuf = cur->get_key(&ksiz, step);
    if (kbuf) {
      outmap["key"] = std::string(kbuf, ksiz);
      delete[] kbuf;
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = cur->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the cur_get_value procedure
  RV do_cur_get_value(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                      kt::TimedDB::Cursor* cur,
                      const std::map<std::string, std::string>& inmap,
                      std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "step");
    bool step = rp ? true : false;
    RV rv;
    size_t vsiz;
    char* vbuf = cur->get_value(&vsiz, step);
    if (vbuf) {
      outmap["value"] = std::string(vbuf, vsiz);
      delete[] vbuf;
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = cur->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the cur_get procedure
  RV do_cur_get(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                kt::TimedDB::Cursor* cur,
                const std::map<std::string, std::string>& inmap,
                std::map<std::string, std::string>& outmap) {
    if (!cur) {
      set_message(outmap, "ERROR", "no such cursor");
      return kt::RPCClient::RVEINVALID;
    }
    const char* rp = kt::strmapget(inmap, "step");
    bool step = rp ? true : false;
    RV rv;
    size_t ksiz, vsiz;
    const char* vbuf;
    int64_t xt;
    char* kbuf = cur->get(&ksiz, &vbuf, &vsiz, &xt, step);
    if (kbuf) {
      outmap["key"] = std::string(kbuf, ksiz);
      outmap["value"] = std::string(vbuf, vsiz);
      if (xt < kt::TimedDB::XTMAX) kc::strprintf(&outmap["xt"], "%lld", (long long)xt);
      delete[] kbuf;
      rv = kt::RPCClient::RVSUCCESS;
    } else {
      const kc::BasicDB::Error& e = cur->error();
      set_db_error(outmap, e);
      if (e == kc::BasicDB::Error::NOREC) {
        rv = kt::RPCClient::RVELOGIC;
      } else {
        log_db_error(serv, e);
        rv = kt::RPCClient::RVEINTERNAL;
      }
    }
    return rv;
  }
  // process the GET method
  int32_t do_rest_get(kt::HTTPServer* serv, kt::HTTPServer::Session* sess,
                      kt::TimedDB* db, const char* kbuf, size_t ksiz,
                      const std::map<std::string, std::string>& reqheads,
                      const std::string& reqbody,
                      std::map<std::string, std::string>& resheads,
                      std::string& resbody,
                      const std::map<std::string, std::string>& misc) {
    int32_t code;
    size_t vsiz;
    int64_t xt;
    const char* vbuf = db->get(kbuf, ksiz, &vsiz, &xt);
    if (vbuf) {
      resbody.append(vbuf, vsiz);
      if (xt < kt::TimedDB::XTMAX) {
        char buf[48];
        kt::datestrhttp(xt, 0, buf);
        resheads["x-kt-xt"] = buf;
      }
      delete[] vbuf;
      code = 200;
    } else {
      const kc::BasicDB::Error& e = db->error();
      kc::strprintf(&resheads["x-kt-error"], "DB: %d: %s: %s", e.code(), e.name(), e.message());
      if (e == kc::BasicDB::Error::NOREC) {
        code = 404;
      } else {
        log_db_error(serv, e);
        code = 500;
      }
    }
    return code;
  }
  // process the HEAD method
  int32_t do_rest_head(kt::HTTPServer* serv, kt::HTTPServer::Session* sess,
                       kt::TimedDB* db, const char* kbuf, size_t ksiz,
                       const std::map<std::string, std::string>& reqheads,
                       const std::string& reqbody,
                       std::map<std::string, std::string>& resheads,
                       std::string& resbody,
                       const std::map<std::string, std::string>& misc) {
    int32_t code;
    size_t vsiz;
    int64_t xt;
    const char* vbuf = db->get(kbuf, ksiz, &vsiz, &xt);
    if (vbuf) {
      if (xt < kt::TimedDB::XTMAX) {
        char buf[48];
        kt::datestrhttp(xt, 0, buf);
        resheads["x-kt-xt"] = buf;
      }
      kc::strprintf(&resheads["content-length"], "%lld", (long long)vsiz);
      delete[] vbuf;
      code = 200;
    } else {
      const kc::BasicDB::Error& e = db->error();
      kc::strprintf(&resheads["x-kt-error"], "DB: %d: %s: %s", e.code(), e.name(), e.message());
      resheads["content-length"] = "0";
      if (e == kc::BasicDB::Error::NOREC) {
        code = 404;
      } else {
        log_db_error(serv, e);
        code = 500;
      }
    }
    return code;
  }
  // process the PUT method
  int32_t do_rest_put(kt::HTTPServer* serv, kt::HTTPServer::Session* sess,
                      kt::TimedDB* db, const char* kbuf, size_t ksiz,
                      const std::map<std::string, std::string>& reqheads,
                      const std::string& reqbody,
                      std::map<std::string, std::string>& resheads,
                      std::string& resbody,
                      const std::map<std::string, std::string>& misc) {
    const char* rp = kt::strmapget(reqheads, "x-kt-xt");
    int64_t xt = rp ? kt::strmktime(rp) : -1;
    xt = xt > 0 && xt < kt::TimedDB::XTMAX ? -xt : INT64_MAX;
    int32_t code;
    if (db->set(kbuf, ksiz, reqbody.data(), reqbody.size(), xt)) {
      const char* url = kt::strmapget(misc, "url");
      if (url) resheads["location"] = url;
      code = 201;
    } else {
      const kc::BasicDB::Error& e = db->error();
      kc::strprintf(&resheads["x-kt-error"], "DB: %d: %s: %s", e.code(), e.name(), e.message());
      log_db_error(serv, e);
      code = 500;
    }
    return code;
  }
  // process the DELETE method
  int32_t do_rest_delete(kt::HTTPServer* serv, kt::HTTPServer::Session* sess,
                         kt::TimedDB* db, const char* kbuf, size_t ksiz,
                         const std::map<std::string, std::string>& reqheads,
                         const std::string& reqbody,
                         std::map<std::string, std::string>& resheads,
                         std::string& resbody,
                         const std::map<std::string, std::string>& misc) {
    int32_t code;
    if (db->remove(kbuf, ksiz)) {
      code = 204;
    } else {
      const kc::BasicDB::Error& e = db->error();
      kc::strprintf(&resheads["x-kt-error"], "DB: %d: %s: %s", e.code(), e.name(), e.message());
      if (e == kc::BasicDB::Error::NOREC) {
        code = 404;
      } else {
        log_db_error(serv, e);
        code = 500;
      }
    }
    return code;
  }
  // session local storage
  class SLS : public kt::RPCServer::Session::Data {
    friend class Worker;
  private:
    SLS() : curs_() {}
    ~SLS() {
      std::map<int64_t, kt::TimedDB::Cursor*>::iterator it = curs_.begin();
      std::map<int64_t, kt::TimedDB::Cursor*>::iterator itend = curs_.end();
      while (it != itend) {
        kt::TimedDB::Cursor* cur = it->second;
        delete cur;
        it++;
      }
    }
    static SLS* create(kt::RPCServer::Session* sess) {
      SLS* sls = (SLS*)sess->data();
      if (!sls) {
        sls = new SLS();
        sess->set_data(sls);
      }
      return sls;
    }
    std::map<int64_t, kt::TimedDB::Cursor*> curs_;
  };
  int32_t thnum_;
  kt::TimedDB* const dbs_;
  const int32_t dbnum_;
  const std::map<std::string, int32_t>& dbmap_;
  const int32_t omode_;
  const char* cmdpath_;
  ScriptProcessor* scrprocs_;
  kc::AtomicInt64 idlecnt_;
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
          " [-ord] [-oat|-oas|-onl|-otl|-onr] [-tp] [-dmn] [-pid file] [-cmd dir] [-scr file]"
          " [db...]\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// kill the running server
static void killserver(int signum) {
  if (g_serv) {
    g_serv->stop();
    g_serv = NULL;
    if (g_daemon && signum == SIGHUP) g_restart = true;
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
  int32_t omode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE;
  int32_t opts = 0;
  bool dmn = false;
  const char* pidpath = NULL;
  const char* cmdpath = NULL;
  const char* scrpath = NULL;
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
      } else if (!std::strcmp(argv[i], "-ord")) {
        omode &= ~kc::BasicDB::OWRITER;
        omode |= kc::BasicDB::OREADER;
      } else if (!std::strcmp(argv[i], "-oat")) {
        omode |= kc::BasicDB::OAUTOTRAN;
      } else if (!std::strcmp(argv[i], "-oas")) {
        omode |= kc::BasicDB::OAUTOSYNC;
      } else if (!std::strcmp(argv[i], "-onl")) {
        omode |= kc::BasicDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        omode |= kc::BasicDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        omode |= kc::BasicDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-tp")) {
        opts |= kt::TimedDB::TPERSIST;
      } else if (!std::strcmp(argv[i], "-dmn")) {
        dmn = true;
      } else if (!std::strcmp(argv[i], "-pid")) {
        if (++i >= argc) usage();
        pidpath = argv[i];
      } else if (!std::strcmp(argv[i], "-cmd")) {
        if (++i >= argc) usage();
        cmdpath = argv[i];
      } else if (!std::strcmp(argv[i], "-scr")) {
        if (++i >= argc) usage();
        scrpath = argv[i];
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
  int32_t rv = proc(dbpaths, host, port, tout, thnum, logpath, logkinds, omode, opts,
                    dmn, pidpath, cmdpath, scrpath);
  return rv;
}


// perform rpc command
static int32_t proc(const std::vector<std::string>& dbpaths,
                    const char* host, int32_t port, double tout, int32_t thnum,
                    const char* logpath, uint32_t logkinds, int32_t omode, int32_t opts,
                    bool dmn, const char* pidpath, const char* cmdpath, const char* scrpath) {
  g_daemon = false;
  if (dmn) {
    if (kc::File::PATHCHR == '/') {
      if (logpath && *logpath != kc::File::PATHCHR) {
        eprintf("%s: %s: a daemon can accept absolute path only\n", g_progname, logpath);
        return 1;
      }
      if (pidpath && *pidpath != kc::File::PATHCHR) {
        eprintf("%s: %s: a daemon can accept absolute path only\n", g_progname, pidpath);
        return 1;
      }
      if (cmdpath && *cmdpath != kc::File::PATHCHR) {
        eprintf("%s: %s: a daemon can accept absolute path only\n", g_progname, cmdpath);
        return 1;
      }
      if (scrpath && *scrpath != kc::File::PATHCHR) {
        eprintf("%s: %s: a daemon can accept absolute path only\n", g_progname, scrpath);
        return 1;
      }
    }
    if (!kt::daemonize()) {
      eprintf("%s: switching to a daemon failed\n", g_progname);
      return 1;
    }
    g_procid = kc::getpid();
    g_daemon = true;
  }
  if (!cmdpath) cmdpath = kc::File::CDIRSTR;
  kc::File::Status sbuf;
  if (!kc::File::status(cmdpath, &sbuf) || !sbuf.isdir) {
    eprintf("%s: %s: no such directory\n", g_progname, cmdpath);
    return 1;
  }
  if (scrpath && !kc::File::status(scrpath)) {
    eprintf("%s: %s: no such file\n", g_progname, scrpath);
    return 1;
  }
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
      serv.log(kt::RPCServer::Logger::ERROR, "%s: unknown host", g_progname, host);
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
    if (!dbs[i].open(dbpath, omode)) {
      const kc::BasicDB::Error& e = dbs[i].error();
      serv.log(kt::RPCServer::Logger::ERROR, "%s: could not open a database file: %s: %s",
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
  ScriptProcessor* scrprocs = NULL;
  if (scrpath) {
    serv.log(kt::RPCServer::Logger::SYSTEM, "loading a script file: path=%s", scrpath);
    scrprocs = new ScriptProcessor[thnum];
    for (int32_t i = 0; i < thnum; i++) {
      if (!scrprocs[i].set_resources(i, &serv, dbs, dbnum, &dbmap)) {
        serv.log(kt::RPCServer::Logger::ERROR, "could not initialize the scripting processor");
        delete[] scrprocs;
        delete[] dbs;
        return 1;
      }
      if (!scrprocs[i].load(scrpath)) {
        serv.log(kt::RPCServer::Logger::ERROR, "%s: could not load a script file", scrpath);
        delete[] scrprocs;
        delete[] dbs;
        return 1;
      }
    }
  }
  Worker worker(thnum, dbs, dbnum, dbmap, omode, cmdpath, scrprocs);
  serv.set_worker(&worker, thnum);
  if (pidpath) {
    char numbuf[kc::NUMBUFSIZ];
    size_t nsiz = std::sprintf(numbuf, "%d\n", g_procid);
    kc::File::write_file(pidpath, numbuf, nsiz);
  }
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
  delete[] scrprocs;
  delete[] dbs;
  if (pidpath) kc::File::remove(pidpath);
  serv.log(kt::RPCServer::Logger::SYSTEM, "================ [FINISH]: pid=%d", g_procid);
  return err ? 1 : 0;
}



// END OF FILE
