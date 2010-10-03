/*************************************************************************************************
 * The test cases of the remote database
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


#include <ktremotedb.h>
#include "cmdcommon.h"


// global variables
const char* g_progname;                  // program name
uint32_t g_randseed;                     // random seed
int64_t g_memusage;                      // memory usage


// function prototypes
int main(int argc, char** argv);
static void usage();
static void dberrprint(kt::RemoteDB* db, int32_t line, const char* func);
static void dbmetaprint(kt::RemoteDB* db, bool verbose);
static int32_t runorder(int argc, char** argv);
static int32_t procorder(int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                         const char* host, int32_t port, double tout);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  const char* ebuf = kc::getenv("KTRNDSEED");
  g_randseed = ebuf ? (uint32_t)kc::atoi(ebuf) : (uint32_t)(kc::time() * 1000);
  mysrand(g_randseed);
  g_memusage = memusage();
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "order")) {
    rv = runorder(argc, argv);
  } else {
    usage();
  }
  if (rv != 0) {
    iprintf("FAILED: KTRNDSEED=%u PID=%ld", g_randseed, (long)kc::getpid());
    for (int32_t i = 0; i < argc; i++) {
      iprintf(" %s", argv[i]);
    }
    iprintf("\n\n");
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: test cases of the remote database of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s order [-th num] [-rnd] [-set|-get|-rem|-etc]"
          " [-host str] [-port num] [-tout num] rnum\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print the error message of a database
static void dberrprint(kt::RemoteDB* db, int32_t line, const char* func) {
  const kt::RemoteDB::Error& err = db->error();
  iprintf("%s: %d: %s: %s: %d: %s: %s\n",
          g_progname, line, func, db->expression().c_str(),
          err.code(), err.name(), err.message());
}


// print members of a database
static void dbmetaprint(kt::RemoteDB* db, bool verbose) {
  if (verbose) {
    std::map<std::string, std::string> status;
    if (db->status(&status)) {
      std::map<std::string, std::string>::iterator it = status.begin();
      std::map<std::string, std::string>::iterator itend = status.end();
      while (it != itend) {
        iprintf("%s: %s\n", it->first.c_str(), it->second.c_str());
        it++;
      }
    }
  } else {
    iprintf("count: %lld\n", (long long)db->count());
    iprintf("size: %lld\n", (long long)db->size());
  }
  int64_t musage = memusage();
  if (musage > 0) iprintf("memory: %lld\n", (long long)(musage - g_memusage));
}


// parse arguments of order command
static int32_t runorder(int argc, char** argv) {
  bool argbrk = false;
  const char* rstr = NULL;
  int32_t thnum = 1;
  bool rnd = false;
  int32_t mode = 0;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-rnd")) {
        rnd = true;
      } else if (!std::strcmp(argv[i], "-set")) {
        mode = 's';
      } else if (!std::strcmp(argv[i], "-get")) {
        mode = 'g';
      } else if (!std::strcmp(argv[i], "-rem")) {
        mode = 'r';
      } else if (!std::strcmp(argv[i], "-etc")) {
        mode = 'e';
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else {
        usage();
      }
    } else if (!rstr) {
      argbrk = false;
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procorder(rnum, thnum, rnd, mode, host, port, tout);
  return rv;
}


// perform rpc command
static int32_t procorder(int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                         const char* host, int32_t port, double tout) {
  iprintf("<In-order Test>\n  seed=%u  rnum=%lld  thnum=%d  rnd=%d  mode=%d  host=%s  port=%d"
          "  tout=%f\n\n", g_randseed, (long long)rnum, thnum, rnd, mode, host, port, tout);
  bool err = false;
  iprintf("opening the database:\n");
  double stime = kc::time();
  kt::RemoteDB* dbs = new kt::RemoteDB[thnum];
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].open(host, port, tout)) {
      dberrprint(dbs + i, __LINE__, "DB::open");
      err = true;
    }
  }
  if (!dbs[0].clear()) {
    dberrprint(dbs, __LINE__, "DB::clear");
    err = true;
  }
  double etime = kc::time();
  iprintf("time: %.3f\n", etime - stime);
  if (mode == 0 || mode == 's' || mode == 'e') {
    iprintf("setting records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
    public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
    private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : INT64_MAX;
          if (!db_->set(kbuf, ksiz, kbuf, ksiz, xt)) {
            dberrprint(db_, __LINE__, "DB::set");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            iputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) iprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 's');
    iprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    iprintf("adding records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
    public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
    private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : INT64_MAX;
          if (!db_->add(kbuf, ksiz, kbuf, ksiz, xt) &&
              db_->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "DB::add");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            iputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) iprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, false);
    iprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    iprintf("appending records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
    public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
    private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : INT64_MAX;
          if (!db_->append(kbuf, ksiz, kbuf, ksiz, xt)) {
            dberrprint(db_, __LINE__, "DB::set");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            iputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) iprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, false);
    iprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'g' || mode == 'e') {
    iprintf("geting records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
    public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
    private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          size_t vsiz;
          char* vbuf = db_->get(kbuf, ksiz, &vsiz);
          if (vbuf) {
            if (vsiz < ksiz || std::memcmp(vbuf, kbuf, ksiz)) {
              dberrprint(db_, __LINE__, "DB::get");
              err_ = true;
            }
            delete[] vbuf;
          } else if (!rnd_ || db_->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "DB::get");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            iputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) iprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 'g');
    iprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    iprintf("traversing the database by the outer cursor:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
    public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
    private:
      void run() {
        kt::RemoteDB::Cursor* cur = db_->cursor();
        if (!cur->jump() && cur->error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(db_, __LINE__, "Cursor::jump");
          err_ = true;
        }
        int64_t cnt = 0;
        const char* kbuf;
        size_t ksiz;
        while ((kbuf = cur->get_key(&ksiz)) != NULL) {
          cnt++;
          if (rnd_) {
            switch (myrand(5)) {
              case 0: {
                char vbuf[RECBUFSIZ];
                size_t vsiz = std::sprintf(vbuf, "%lld", cnt);
                if (!cur->set_value(vbuf, vsiz, myrand(600) + 1, myrand(2) == 0) &&
                    cur->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "Cursor::set_value");
                  err_ = true;
                }
                break;
              }
              case 1: {
                if (!cur->remove() && cur->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "Cursor::remove");
                  err_ = true;
                }
                break;
              }
              case 2: {
                size_t rsiz, vsiz;
                const char* vbuf;
                int64_t xt;
                char* rbuf = cur->get(&rsiz, &vbuf, &vsiz, &xt, myrand(2) == 0);
                if (rbuf ) {
                  delete[] rbuf;
                } else if (cur->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "Cursor::et");
                  err_ = true;
                }
                break;
              }
            }
          } else {
            size_t vsiz;
            char* vbuf = cur->get_value(&vsiz);
            if (vbuf) {
              delete[] vbuf;
            } else {
              dberrprint(db_, __LINE__, "Cursor::get_value");
              err_ = true;
            }
          }
          delete[] kbuf;
          if (!cur->step() && cur->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "Cursor::step");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && cnt % (rnum_ / 250) == 0) {
            iputchar('.');
            if (cnt == rnum_ || cnt % (rnum_ / 10) == 0) iprintf(" (%08lld)\n", (long long)cnt);
          }
        }
        if (cur->error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(db_, __LINE__, "Cursor::get_key");
          err_ = true;
        }
        if (!rnd_ && cnt != db_->count()) {
          dberrprint(db_, __LINE__, "Cursor::get_key");
          err_ = true;
        }
        if (id_ < 1) iprintf(" (end)\n");
        delete cur;
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, false);
    iprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'r' || mode == 'e') {
    iprintf("removing records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
    public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), mode_(0), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                     kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        mode_ = mode;
        db_ = db;
      }
      bool error() {
        return err_;
      }
    private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          if (!db_->remove(kbuf, ksiz) &&
              ((!rnd_ && mode_ != 'e') || db_->error() != kt::RemoteDB::Error::LOGIC)) {
            dberrprint(db_, __LINE__, "DB::remove");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            iputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) iprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      int32_t mode_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, mode, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 'r' || mode == 'e');
    iprintf("time: %.3f\n", etime - stime);
  }
  iprintf("closing the database:\n");
  stime = kc::time();
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].close()) {
      dberrprint(dbs + i, __LINE__, "DB::close");
      err = true;
    }
  }
  etime = kc::time();
  iprintf("time: %.3f\n", etime - stime);
  delete[] dbs;
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



// END OF FILE
