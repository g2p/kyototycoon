/*************************************************************************************************
 * The command line utility of the remote database
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


// function prototypes
int main(int argc, char** argv);
static void usage();
static void dberrprint(kt::RemoteDB* db, const char* info);
static int32_t runreport(int argc, char** argv);
static int32_t runinform(int argc, char** argv);
static int32_t runclear(int argc, char** argv);
static int32_t runsync(int argc, char** argv);
static int32_t runset(int argc, char** argv);
static int32_t runremove(int argc, char** argv);
static int32_t runget(int argc, char** argv);
static int32_t runlist(int argc, char** argv);
static int32_t procreport(const char* host, int32_t port, double tout);
static int32_t procinform(const char* host, int32_t port, double tout, const char* dbexpr,
                          bool st);
static int32_t procclear(const char* host, int32_t port, double tout, const char* dbexpr);
static int32_t procsync(const char* host, int32_t port, double tout, const char* dbexpr,
                        bool hard, const char* cmd);
static int32_t procset(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
                       const char* host, int32_t port, double tout, const char* dbexpr,
                       int32_t mode, int64_t xt);
static int32_t procremove(const char* kbuf, size_t ksiz,
                          const char* host, int32_t port, double tout, const char* dbexpr);
static int32_t procget(const char* kbuf, size_t ksiz,
                       const char* host, int32_t port, double tout, const char* dbexpr,
                       bool px, bool pt, bool pz);
static int32_t proclist(const char* kbuf, size_t ksiz,
                        const char* host, int32_t port, double tout, const char* dbexpr,
                        bool des, int64_t max, bool pv, bool px, bool pt);


// print the usage and exit
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "report")) {
    rv = runreport(argc, argv);
  } else if (!std::strcmp(argv[1], "inform")) {
    rv = runinform(argc, argv);
  } else if (!std::strcmp(argv[1], "clear")) {
    rv = runclear(argc, argv);
  } else if (!std::strcmp(argv[1], "sync")) {
    rv = runsync(argc, argv);
  } else if (!std::strcmp(argv[1], "set")) {
    rv = runset(argc, argv);
  } else if (!std::strcmp(argv[1], "remove")) {
    rv = runremove(argc, argv);
  } else if (!std::strcmp(argv[1], "get")) {
    rv = runget(argc, argv);
  } else if (!std::strcmp(argv[1], "list")) {
    rv = runlist(argc, argv);
  } else if (!std::strcmp(argv[1], "version") || !std::strcmp(argv[1], "--version")) {
    printversion();
  } else {
    usage();
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: the command line utility of the remote database of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s report [-host str] [-port num] [-tout num]\n", g_progname);
  eprintf("  %s inform [-host str] [-port num] [-tout num] [-db str] [-st]\n", g_progname);
  eprintf("  %s clear [-host str] [-port num] [-tout num] [-db str]\n", g_progname);
  eprintf("  %s sync [-host str] [-port num] [-tout num] [-db str] [-hard] [-cmd str]\n",
          g_progname);
  eprintf("  %s set [-host str] [-port num] [-tout num] [-db str] [-add|-rep|-app|-inci|-incd]"
          " [-sx] [-xt num] key value\n", g_progname);
  eprintf("  %s remove [-host str] [-port num] [-tout num] [-db str] [-sx] key\n", g_progname);
  eprintf("  %s get [-host str] [-port num] [-tout num] [-db str] [-sx] [-px] [-pt] [-pz] key\n",
          g_progname);
  eprintf("  %s list [-host str] [-port num] [-tout num] [-db str] [-des] [-max num]"
          " [-sx] [-pv] [-px] [-pt] [key]\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print error message of database
static void dberrprint(kt::RemoteDB* db, const char* info) {
  const kt::RemoteDB::Error& err = db->error();
  eprintf("%s: %s: %s: %d: %s: %s\n",
          g_progname, info, db->expression().c_str(), err.code(), err.name(), err.message());
}


// parse arguments of report command
static int32_t runreport(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  for (int32_t i = 2; i < argc; i++) {
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
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  int32_t rv = procreport(host, port, tout);
  return rv;
}


// parse arguments of inform command
static int32_t runinform(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  bool st = false;
  for (int32_t i = 2; i < argc; i++) {
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
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-st")) {
        st = true;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  int32_t rv = procinform(host, port, tout, dbexpr, st);
  return rv;
}


// parse arguments of clear command
static int32_t runclear(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  for (int32_t i = 2; i < argc; i++) {
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
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  int32_t rv = procclear(host, port, tout, dbexpr);
  return rv;
}


// parse arguments of sync command
static int32_t runsync(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  bool hard = false;
  const char* cmd = "";
  for (int32_t i = 2; i < argc; i++) {
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
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-hard")) {
        hard = true;
      } else if (!std::strcmp(argv[i], "-cmd")) {
        if (++i >= argc) usage();
        cmd = argv[i];
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  int32_t rv = procsync(host, port, tout, dbexpr, hard, cmd);
  return rv;
}


// parse arguments of set command
static int32_t runset(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* vstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  int32_t mode = 0;
  bool sx = false;
  int64_t xt = INT64_MAX;
  for (int32_t i = 2; i < argc; i++) {
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
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-add")) {
        mode = 'a';
      } else if (!std::strcmp(argv[i], "-rep")) {
        mode = 'r';
      } else if (!std::strcmp(argv[i], "-app")) {
        mode = 'c';
      } else if (!std::strcmp(argv[i], "-inci")) {
        mode = 'i';
      } else if (!std::strcmp(argv[i], "-incd")) {
        mode = 'd';
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-xt")) {
        if (++i >= argc) usage();
        xt = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else if (!vstr) {
      vstr = argv[i];
    } else {
      usage();
    }
  }
  if (!kstr || !vstr) usage();
  char* kbuf;
  size_t ksiz;
  char* vbuf;
  size_t vsiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
    vbuf = kc::hexdecode(vstr, &vsiz);
    vstr = vbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
    vsiz = std::strlen(vstr);
    vbuf = NULL;
  }
  int32_t rv = procset(kstr, ksiz, vstr, vsiz, host, port, tout, dbexpr, mode, xt);
  delete[] kbuf;
  delete[] vbuf;
  return rv;
}


// parse arguments of remove command
static int32_t runremove(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  bool sx = false;
  for (int32_t i = 2; i < argc; i++) {
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
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else {
        usage();
      }
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  int32_t rv = procremove(kstr, ksiz, host, port, tout, dbexpr);
  delete[] kbuf;
  return rv;
}


// parse arguments of get command
static int32_t runget(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  bool sx = false;
  bool px = false;
  bool pt = false;
  bool pz = false;
  for (int32_t i = 2; i < argc; i++) {
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
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else if (!std::strcmp(argv[i], "-pt")) {
        pt = true;
      } else if (!std::strcmp(argv[i], "-pz")) {
        pz = true;
      } else {
        usage();
      }
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  int32_t rv = procget(kstr, ksiz, host, port, tout, dbexpr, px, pt, pz);
  delete[] kbuf;
  return rv;
}


// parse arguments of list command
static int32_t runlist(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  bool des = false;
  int64_t max = -1;
  bool sx = false;
  bool pv = false;
  bool px = false;
  bool pt = false;
  for (int32_t i = 2; i < argc; i++) {
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
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-des")) {
        des = true;
      } else if (!std::strcmp(argv[i], "-max")) {
        if (++i >= argc) usage();
        max = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-pv")) {
        pv = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else if (!std::strcmp(argv[i], "-pt")) {
        pt = true;
      } else {
        usage();
      }
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else {
      usage();
    }
  }
  char* kbuf = NULL;
  size_t ksiz = 0;
  if (kstr) {
    if (sx) {
      kbuf = kc::hexdecode(kstr, &ksiz);
      kstr = kbuf;
    } else {
      ksiz = std::strlen(kstr);
      kbuf = new char[ksiz+1];
      std::memcpy(kbuf, kstr, ksiz);
      kbuf[ksiz] = '\0';
    }
  }
  int32_t rv = proclist(kbuf, ksiz, host, port, tout, dbexpr, des, max, pv, px, pt);
  delete[] kbuf;
  return rv;
}


// perform report command
static int32_t procreport(const char* host, int32_t port, double tout) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  std::map<std::string, std::string> status;
  if (db.report(&status)) {
    std::map<std::string, std::string>::iterator it = status.begin();
    std::map<std::string, std::string>::iterator itend = status.end();
    while (it != itend) {
      iprintf("%s: %s\n", it->first.c_str(), it->second.c_str());
      it++;
    }
  } else {
    dberrprint(&db, "DB::status failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform inform command
static int32_t procinform(const char* host, int32_t port, double tout, const char* dbexpr,
                          bool st) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  std::map<std::string, std::string> status;
  if (db.status(&status)) {
    if (st) {
      std::map<std::string, std::string>::iterator it = status.begin();
      std::map<std::string, std::string>::iterator itend = status.end();
      while (it != itend) {
        iprintf("%s: %s\n", it->first.c_str(), it->second.c_str());
        it++;
      }
    } else {
      iprintf("count: %s\n", status["count"].c_str());
      iprintf("size: %s\n", status["size"].c_str());
    }
  } else {
    dberrprint(&db, "DB::status failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform clear command
static int32_t procclear(const char* host, int32_t port, double tout, const char* dbexpr) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (!db.clear()) {
    dberrprint(&db, "DB::clear failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform sync command
static int32_t procsync(const char* host, int32_t port, double tout, const char* dbexpr,
                        bool hard, const char* cmd) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (!db.synchronize(hard, cmd)) {
    dberrprint(&db, "DB::synchronize failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform set command
static int32_t procset(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
                       const char* host, int32_t port, double tout, const char* dbexpr,
                       int32_t mode, int64_t xt) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  switch (mode) {
    default: {
      if (!db.set(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::set failed");
        err = true;
      }
      break;
    }
    case 'a': {
      if (!db.add(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::add failed");
        err = true;
      }
      break;
    }
    case 'r': {
      if (!db.replace(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::replace failed");
        err = true;
      }
      break;
    }
    case 'c': {
      if (!db.append(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::append failed");
        err = true;
      }
      break;
    }
    case 'i': {
      int64_t onum = db.increment(kbuf, ksiz, kc::atoi(vbuf), xt);
      if (onum == INT64_MIN) {
        dberrprint(&db, "DB::increment failed");
        err = true;
      } else {
        iprintf("%lld\n", (long long)onum);
      }
      break;
    }
    case 'd': {
      double onum = db.increment_double(kbuf, ksiz, kc::atof(vbuf), xt);
      if (kc::chknan(onum)) {
        dberrprint(&db, "DB::increment_double failed");
        err = true;
      } else {
        iprintf("%f\n", onum);
      }
      break;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform remove command
static int32_t procremove(const char* kbuf, size_t ksiz,
                          const char* host, int32_t port, double tout, const char* dbexpr) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (!db.remove(kbuf, ksiz)) {
    dberrprint(&db, "DB::remove failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform get command
static int32_t procget(const char* kbuf, size_t ksiz,
                       const char* host, int32_t port, double tout, const char* dbexpr,
                       bool px, bool pt, bool pz) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  size_t vsiz;
  int64_t xt;
  char* vbuf = db.get(kbuf, ksiz, &vsiz, &xt);
  if (vbuf) {
    printdata(vbuf, vsiz, px);
    if (pt) iprintf("\t%lld", (long long)xt);
    if (!pz) iprintf("\n");
    delete[] vbuf;
  } else {
    dberrprint(&db, "DB::get failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform list command
static int32_t proclist(const char* kbuf, size_t ksiz,
                        const char* host, int32_t port, double tout, const char* dbexpr,
                        bool des, int64_t max, bool pv, bool px, bool pt) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (max < 0) max = INT64_MAX;
  kt::RemoteDB::Cursor cur(&db);
  if (des) {
    if (kbuf) {
      if (!cur.jump(kbuf, ksiz) && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    } else {
      if (!cur.jump_back() && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    }
    while (!err && max > 0) {
      size_t ksiz, vsiz;
      const char* vbuf;
      int64_t xt;
      char* kbuf = cur.get(&ksiz, &vbuf, &vsiz, &xt, false);
      if (kbuf) {
        printdata(kbuf, ksiz, px);
        if (pv) {
          iprintf("\t");
          printdata(vbuf, vsiz, px);
        }
        if (pt) iprintf("\t%lld", (long long)xt);
        iprintf("\n");
        delete[] kbuf;
      } else {
        if (db.error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(&db, "Cursor::get failed");
          err = true;
        }
        break;
      }
      cur.step_back();
      max--;
    }
  } else {
    if (kbuf) {
      if (!cur.jump(kbuf, ksiz) && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    } else {
      if (!cur.jump() && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    }
    while (!err && max > 0) {
      size_t ksiz, vsiz;
      const char* vbuf;
      int64_t xt;
      char* kbuf = cur.get(&ksiz, &vbuf, &vsiz, &xt, true);
      if (kbuf) {
        printdata(kbuf, ksiz, px);
        if (pv) {
          iprintf("\t");
          printdata(vbuf, vsiz, px);
        }
        if (pt) iprintf("\t%lld", (long long)xt);
        iprintf("\n");
        delete[] kbuf;
      } else {
        if (db.error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(&db, "Cursor::get failed");
          err = true;
        }
        break;
      }
      max--;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}



// END OF FILE
