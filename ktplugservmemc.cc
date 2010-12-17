/*************************************************************************************************
 * A pluggable server for the memcached protocol
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


#include <ktplugserv.h>

namespace kc = kyotocabinet;
namespace kt = kyototycoon;


// pluggable server for the memcached protocol
class MemcacheServer : public kt::PluggableServer {
private:
  class Server;
  class Worker;
public:
  // default constructor
  explicit MemcacheServer() :
    dbary_(NULL), dbnum_(0), logger_(NULL), expr_(""), host_(""), port_(0), opts_(0),
    stime_(0), serv_(), worker_(this) {
    _assert_(true);
  }
  // destructor
  ~MemcacheServer() {
    _assert_(true);
  }
  // configure server settings
  void configure(kt::TimedDB* dbary, size_t dbnum,
                 kt::ThreadedServer::Logger* logger, uint32_t logkinds,
                 const char* expr) {
    _assert_(dbary && logger && expr);
    dbary_ = dbary;
    dbnum_ = dbnum;
    logger_ = logger;
    logkinds_ = logkinds;
    expr_ = expr;
    serv_.set_logger(logger_, logkinds_);
    serv_.log(kt::ThreadedServer::Logger::SYSTEM,
              "the plug-in memcached server configured: expr=%s", expr);
    host_ = "";
    port_ = 0;
    opts_ = 0;
    std::vector<std::string> elems;
    kc::strsplit(expr_.c_str(), '#', &elems);
    std::vector<std::string>::iterator it = elems.begin();
    std::vector<std::string>::iterator itend = elems.end();
    while (it != itend) {
      std::vector<std::string> fields;
      if (kc::strsplit(*it, '=', &fields) > 1) {
        const char* key = fields[0].c_str();
        const char* value = fields[1].c_str();
        if (!std::strcmp(key, "host")) {
          host_ = value;
        } else if (!std::strcmp(key, "port")) {
          port_ = kc::atoi(value);
        } else if (!std::strcmp(key, "opts") || !std::strcmp(key, "options")) {
          if (std::strchr(value, 'f')) opts_ |= TFLAGS;
        }
      }
      it++;
    }
    if (port_ < 1) port_ = 11211;
    stime_ = kc::time();
  }
  // start the server
  bool start() {
    _assert_(true);
    std::string addr;
    if (!host_.empty()) {
      addr = kt::Socket::get_host_address(host_);
      if (addr.empty()) {
        serv_.log(kt::ThreadedServer::Logger::ERROR, "unknown host: %s", host_.c_str());
        return false;
      }
    }
    std::string nexpr;
    kc::strprintf(&nexpr, "%s:%d", addr.c_str(), port_);
    serv_.set_network(nexpr, 30);
    serv_.set_worker(&worker_, 8);
    return serv_.start();
  }
  // stop the server
  bool stop() {
    _assert_(true);
    return serv_.stop();
  }
  // finish the server
  bool finish() {
    _assert_(true);
    return serv_.finish();
  }
private:
  // tuning options
  enum Option {
    TFLAGS = 1 << 1
  };
  // server implementation
  class Worker : public kt::ThreadedServer::Worker {
  public:
    // constructor
    Worker(MemcacheServer* serv) : serv_(serv) {}
  private:
    // process each request.
    bool process(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess) {
      kt::TimedDB* db = serv_->dbary_;
      bool keep = false;
      char line[8192];
      if (sess->receive_line(line, sizeof(line))) {
        keep = true;
        std::vector<std::string> tokens;
        kt::strtokenize(line, &tokens);
        const std::string& cmd = tokens.empty() ? "" : tokens.front();
        if (cmd == "set") {
          keep = do_set(serv, sess, tokens, db);
        } else if (cmd == "add") {
          keep = do_add(serv, sess, tokens, db);
        } else if (cmd == "replace") {
          keep = do_replace(serv, sess, tokens, db);
        } else if (cmd == "get") {
          keep = do_get(serv, sess, tokens, db);
        } else if (cmd == "delete") {
          keep = do_delete(serv, sess, tokens, db);
        } else if (cmd == "incr") {
          keep = do_incr(serv, sess, tokens, db);
        } else if (cmd == "decr") {
          keep = do_decr(serv, sess, tokens, db);
        } else if (cmd == "stats") {
          keep = do_stats(serv, sess, tokens, db);
        } else if (cmd == "flush_all") {
          keep = do_flush_all(serv, sess, tokens, db);
        } else if (cmd == "quit") {
          keep = false;
        } else {
          sess->printf("ERROR\r\n");
        }
        std::string expr = sess->expression();
        serv->log(kt::ThreadedServer::Logger::INFO, "(%s): %s", expr.c_str(), cmd.c_str());
      }
      return keep;
    }
    // log the database error message
    void log_db_error(kt::ThreadedServer* serv, const kc::BasicDB::Error& e) {
      serv->log(kt::ThreadedServer::Logger::ERROR,
                "database error: %d: %s: %s", e.code(), e.name(), e.message());
    }
    // process the set command
    bool do_set(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 5) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      const std::string& key = tokens[1];
      uint32_t flags = kc::atoi(tokens[2].c_str());
      int64_t xt = kc::atoi(tokens[3].c_str());
      int64_t vsiz = kc::atoi(tokens[4].c_str());
      if (xt < 1) {
        xt = INT64_MAX;
      } else if (xt > 1 << 24) {
        xt *= -1;
      }
      if (vsiz < 0 || vsiz > (int64_t)kt::RemoteDB::DATAMAXSIZ) return false;
      bool err = false;
      char* vbuf = new char[vsiz+sizeof(flags)];
      if (sess->receive(vbuf, vsiz)) {
        int32_t c = sess->receive_byte();
        if (c == '\r') c = sess->receive_byte();
        if (c == '\n') {
          if (serv_->opts_ & TFLAGS) {
            kc::writefixnum(vbuf + vsiz, flags, sizeof(flags));
            vsiz += sizeof(flags);
          }
          if (db->set(key.data(), key.size(), vbuf, vsiz, xt)) {
            if (!sess->printf("STORED\r\n")) err = true;
          } else {
            const kc::BasicDB::Error& e = db->error();
            log_db_error(serv, e);
            if (!sess->printf("SERVER_ERROR DB::set failed\r\n")) err = true;
          }
        } else {
          err = true;
        }
      } else {
        err = true;
      }
      delete[] vbuf;
      return !err;
    }
    // process the add command
    bool do_add(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 5) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      const std::string& key = tokens[1];
      uint32_t flags = kc::atoi(tokens[2].c_str());
      int64_t xt = kc::atoi(tokens[3].c_str());
      int64_t vsiz = kc::atoi(tokens[4].c_str());
      if (xt < 1) {
        xt = INT64_MAX;
      } else if (xt > 1 << 24) {
        xt *= -1;
      }
      if (vsiz < 0 || vsiz > (int64_t)kt::RemoteDB::DATAMAXSIZ) return false;
      bool err = false;
      char* vbuf = new char[vsiz+sizeof(flags)];
      if (sess->receive(vbuf, vsiz)) {
        int32_t c = sess->receive_byte();
        if (c == '\r') c = sess->receive_byte();
        if (c == '\n') {
          if (serv_->opts_ & TFLAGS) {
            kc::writefixnum(vbuf + vsiz, flags, sizeof(flags));
            vsiz += sizeof(flags);
          }
          if (db->add(key.data(), key.size(), vbuf, vsiz, xt)) {
            if (!sess->printf("STORED\r\n")) err = true;
          } else {
            const kc::BasicDB::Error& e = db->error();
            if (e == kc::BasicDB::Error::DUPREC) {
              if (!sess->printf("NOT_STORED\r\n")) err = true;
            } else {
              log_db_error(serv, e);
              if (!sess->printf("SERVER_ERROR DB::add failed\r\n")) err = true;
            }
          }
        } else {
          err = true;
        }
      } else {
        err = true;
      }
      delete[] vbuf;
      return !err;
    }
    // process the replace command
    bool do_replace(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                    const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 5) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      const std::string& key = tokens[1];
      uint32_t flags = kc::atoi(tokens[2].c_str());
      int64_t xt = kc::atoi(tokens[3].c_str());
      int64_t vsiz = kc::atoi(tokens[4].c_str());
      if (xt < 1) {
        xt = INT64_MAX;
      } else if (xt > 1 << 24) {
        xt *= -1;
      }
      if (vsiz < 0 || vsiz > (int64_t)kt::RemoteDB::DATAMAXSIZ) return false;
      bool err = false;
      char* vbuf = new char[vsiz+sizeof(flags)];
      if (sess->receive(vbuf, vsiz)) {
        int32_t c = sess->receive_byte();
        if (c == '\r') c = sess->receive_byte();
        if (c == '\n') {
          if (serv_->opts_ & TFLAGS) {
            kc::writefixnum(vbuf + vsiz, flags, sizeof(flags));
            vsiz += sizeof(flags);
          }
          if (db->replace(key.data(), key.size(), vbuf, vsiz, xt)) {
            if (!sess->printf("STORED\r\n")) err = true;
          } else {
            const kc::BasicDB::Error& e = db->error();
            if (e == kc::BasicDB::Error::NOREC) {
              if (!sess->printf("NOT_STORED\r\n")) err = true;
            } else {
              log_db_error(serv, e);
              if (!sess->printf("SERVER_ERROR DB::replace failed\r\n")) err = true;
            }
          }
        } else {
          err = true;
        }
      } else {
        err = true;
      }
      delete[] vbuf;
      return !err;
    }
    // process the get command
    bool do_get(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 1) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      bool err = false;
      std::vector<std::string>::const_iterator it = tokens.begin();
      std::vector<std::string>::const_iterator itend = tokens.end();
      std::string result;
      while (it != itend) {
        size_t vsiz;
        char* vbuf = db->get(it->data(), it->size(), &vsiz);
        if (vbuf) {
          uint32_t flags = 0;
          if ((serv_->opts_ & TFLAGS) && vsiz >= sizeof(flags)) {
            flags = kc::readfixnum(vbuf + vsiz - sizeof(flags), sizeof(flags));
            vsiz -= sizeof(flags);
          }
          kc::strprintf(&result, "VALUE %s %u %llu\r\n",
                        it->c_str(), flags, (unsigned long long)vsiz);
          result.append(vbuf, vsiz);
          result.append("\r\n");
          delete[] vbuf;
        }
        it++;
      }
      kc::strprintf(&result, "END\r\n");
      if (!sess->send(result.data(), result.size())) err = true;
      return !err;
    }
    // process the delete command
    bool do_delete(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                   const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 2) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      const std::string& key = tokens[1];
      bool err = false;
      if (db->remove(key.data(), key.size())) {
        if (!sess->printf("DELETED\r\n")) err = true;
      } else {
        const kc::BasicDB::Error& e = db->error();
        if (e == kc::BasicDB::Error::NOREC) {
          if (!sess->printf("NOT_FOUND\r\n")) err = true;
        } else {
          log_db_error(serv, e);
          if (!sess->printf("SERVER_ERROR DB::remove failed\r\n")) err = true;
        }
      }
      return !err;
    }
    // process the incr command
    bool do_incr(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                 const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 3) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      const std::string& key = tokens[1];
      int64_t num = kc::atoi(tokens[2].c_str());
      bool err = false;
      class Visitor : public kt::TimedDB::Visitor {
      public:
        explicit Visitor(int64_t num, uint8_t opts) :
          num_(num), opts_(opts), hit_(false), nbuf_() {}
        int64_t num() {
          return num_;
        }
        bool hit() {
          return hit_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          hit_ = true;
          if ((opts_ & TFLAGS) && vsiz >= sizeof(uint32_t)) {
            num_ += kc::atoin(vbuf, vsiz - sizeof(uint32_t));
            if (num_ < 0) num_ = 0;
            size_t nsiz = std::sprintf(nbuf_, "%lld", (long long)num_);
            std::memcpy(nbuf_ + nsiz, vbuf + vsiz - sizeof(uint32_t), sizeof(uint32_t));
            nsiz += sizeof(uint32_t);
            *sp = nsiz;
          } else {
            num_ += kc::atoin(vbuf, vsiz);
            if (num_ < 0) num_ = 0;
            *sp = std::sprintf(nbuf_, "%lld", (long long)num_);
          }
          *xtp = -*xtp;
          return nbuf_;
        }
        int64_t num_;
        uint8_t opts_;
        bool hit_;
        char nbuf_[kc::NUMBUFSIZ];
      };
      Visitor visitor(num, serv_->opts_);
      if (db->accept(key.data(), key.size(), &visitor, true)) {
        if (visitor.hit()) {
          if (!sess->printf("%lld\r\n", (long long)visitor.num())) err = true;
        } else {
          if (!sess->printf("NOT_FOUND\r\n")) err = true;
        }
      } else {
        const kc::BasicDB::Error& e = db->error();
        log_db_error(serv, e);
        if (!sess->printf("SERVER_ERROR DB::accept failed\r\n")) err = true;
      }
      return !err;
    }
    // process the decr command
    bool do_decr(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                 const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 3) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      const std::string& key = tokens[1];
      int64_t num = -kc::atoi(tokens[2].c_str());
      bool err = false;
      class Visitor : public kt::TimedDB::Visitor {
      public:
        explicit Visitor(int64_t num, uint8_t opts) :
          num_(num), opts_(opts), hit_(false), nbuf_() {}
        int64_t num() {
          return num_;
        }
        bool hit() {
          return hit_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          hit_ = true;
          if ((opts_ & TFLAGS) && vsiz >= sizeof(uint32_t)) {
            num_ += kc::atoin(vbuf, vsiz - sizeof(uint32_t));
            if (num_ < 0) num_ = 0;
            size_t nsiz = std::sprintf(nbuf_, "%lld", (long long)num_);
            std::memcpy(nbuf_ + nsiz, vbuf + vsiz - sizeof(uint32_t), sizeof(uint32_t));
            nsiz += sizeof(uint32_t);
            *sp = nsiz;
          } else {
            num_ += kc::atoin(vbuf, vsiz);
            if (num_ < 0) num_ = 0;
            *sp = std::sprintf(nbuf_, "%lld", (long long)num_);
          }
          *xtp = -*xtp;
          return nbuf_;
        }
        int64_t num_;
        uint8_t opts_;
        bool hit_;
        char nbuf_[kc::NUMBUFSIZ];
      };
      Visitor visitor(num, serv_->opts_);
      if (db->accept(key.data(), key.size(), &visitor, true)) {
        if (visitor.hit()) {
          if (!sess->printf("%lld\r\n", (long long)visitor.num())) err = true;
        } else {
          if (!sess->printf("NOT_FOUND\r\n")) err = true;
        }
      } else {
        const kc::BasicDB::Error& e = db->error();
        log_db_error(serv, e);
        if (!sess->printf("SERVER_ERROR DB::accept failed\r\n")) err = true;
      }
      return !err;
    }
    // process the stats command
    bool do_stats(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                  const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 1) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      bool err = false;
      std::string result;
      std::map<std::string, std::string> status;
      if (db->status(&status)) {
        kc::strprintf(&result, "STAT pid %lld\r\n", (long long)kc::getpid());
        double now = kc::time();
        kc::strprintf(&result, "STAT uptime %lld\r\n", (long long)(now - serv_->stime_));
        kc::strprintf(&result, "STAT time %lld\r\n", (long long)now);
        kc::strprintf(&result, "STAT version KyotoTycoon/%s\r\n", kt::VERSION);
        kc::strprintf(&result, "STAT pointer_size %d\r\n", (int)(sizeof(void*) * 8));
        kc::strprintf(&result, "STAT curr_items %lld\r\n", (long long)db->count());
        kc::strprintf(&result, "STAT bytes %lld\r\n", (long long)db->size());
        std::map<std::string, std::string>::iterator it = status.begin();
        std::map<std::string, std::string>::iterator itend = status.end();
        while (it != itend) {
          kc::strprintf(&result, "STAT db_%s %s\r\n", it->first.c_str(), it->second.c_str());
          it++;
        }
        kc::strprintf(&result, "END\r\n");
      } else {
        const kc::BasicDB::Error& e = db->error();
        log_db_error(serv, e);
        kc::strprintf(&result, "SERVER_ERROR DB::status failed\r\n");
      }
      if (!sess->send(result.data(), result.size())) err = true;
      return !err;
    }
    // process the flush_all command
    bool do_flush_all(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
                      const std::vector<std::string>& tokens, kt::TimedDB* db) {
      if (tokens.size() < 1) return sess->printf("CLIENT_ERROR invalid parameters\r\n");
      bool err = false;
      std::string result;
      std::map<std::string, std::string> status;
      if (db->clear()) {
        if (!sess->printf("OK\r\n")) err = true;
      } else {
        const kc::BasicDB::Error& e = db->error();
        log_db_error(serv, e);
        if (!sess->printf("SERVER_ERROR DB::clear failed\r\n")) err = true;
      }
      return !err;
    }
    MemcacheServer* serv_;
  };
  kt::TimedDB* dbary_;
  size_t dbnum_;
  kt::ThreadedServer::Logger* logger_;
  uint32_t logkinds_;
  std::string expr_;
  std::string host_;
  int32_t port_;
  uint8_t opts_;
  double stime_;
  kt::ThreadedServer serv_;
  Worker worker_;
};


// initializer called by the main server
extern "C" void* ktservinit() {
  return new MemcacheServer;
}



// END OF FILE
