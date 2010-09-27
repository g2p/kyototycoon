/*************************************************************************************************
 * The test cases of the utility functions
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


// constants
const size_t LOCKSLOTNUM = 128;          // number of lock slots
const size_t FILEIOUNIT = 50;            // file I/O unit size


// global variables
const char* g_progname;                  // program name
uint32_t g_randseed;                     // random seed
int64_t g_memusage;                      // memory usage


// function prototypes
int main(int argc, char** argv);
static void usage();
static void errprint(int32_t line, const char* format, ...);
static int32_t runhttp(int32_t argc, char** argv);
static int32_t prochttp(const char* url, int64_t rnum, int64_t thnum,
                        kt::HTTPClient::Method meth, const char* body,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries, double tout, bool ka);


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
  if (!std::strcmp(argv[1], "http")) {
    rv = runhttp(argc, argv);
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
  eprintf("%s: test cases of the utility functions of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s http [-get|-head|-post|-put|-delete] [-body file] [-ah name value]"
          " [-qs name value] [-tout num] [-ka] url rnum\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print formatted error information string and flush the buffer
static void errprint(int32_t line, const char* format, ...) {
  std::string msg;
  kc::strprintf(&msg, "%s: %d: ", g_progname, line);
  va_list ap;
  va_start(ap, format);
  kc::vstrprintf(&msg, format, ap);
  va_end(ap);
  kc::strprintf(&msg, "\n");
  std::cout << msg;
  std::cout.flush();
}


// parse arguments of http command
static int32_t runhttp(int32_t argc, char** argv) {
  const char* url = NULL;
  const char* rstr = NULL;
  int32_t thnum = 1;
  kt::HTTPClient::Method meth = kt::HTTPClient::MUNKNOWN;
  const char* body = NULL;
  std::map<std::string, std::string> reqheads;
  std::map<std::string, std::string> queries;
  double tout = 0;
  bool ka = false;
  for (int32_t i = 2; i < argc; i++) {
    if (argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-get")) {
        meth = kt::HTTPClient::MGET;
      } else if (!std::strcmp(argv[i], "-head")) {
        meth = kt::HTTPClient::MHEAD;
      } else if (!std::strcmp(argv[i], "-post")) {
        meth = kt::HTTPClient::MPOST;
      } else if (!std::strcmp(argv[i], "-put")) {
        meth = kt::HTTPClient::MPUT;
      } else if (!std::strcmp(argv[i], "-delete")) {
        meth = kt::HTTPClient::MDELETE;
      } else if (!std::strcmp(argv[i], "-body")) {
        if (++i >= argc) usage();
        body = argv[i];
      } else if (!std::strcmp(argv[i], "-ah")) {
        if ((i += 2) >= argc) usage();
        char* name = kc::strdup(argv[i-1]);
        kc::strnrmspc(name);
        kc::strtolower(name);
        reqheads[name] = argv[i];
        delete[] name;
      } else if (!std::strcmp(argv[i], "-qs")) {
        if ((i += 2) >= argc) usage();
        queries[argv[i-1]] = argv[i];
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ka")) {
        ka = true;
      } else {
        usage();
      }
    } else if (!url) {
      url = argv[i];
    } else if (!rstr) {
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!url || !rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = prochttp(url, rnum, thnum, meth, body, &reqheads, &queries, tout, ka);
  return rv;
}


// perform http command
static int32_t prochttp(const char* url, int64_t rnum, int64_t thnum,
                        kt::HTTPClient::Method meth, const char* body,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries, double tout, bool ka) {
  const char* istr = body && *body == '@' ? body + 1 : NULL;
  std::istream *is;
  std::ifstream ifs;
  std::istringstream iss(istr ? istr : "");
  if (body) {
    if (istr) {
      is = &iss;
    } else {
      ifs.open(body, std::ios_base::in | std::ios_base::binary);
      if (!ifs) {
        errprint(__LINE__, "%s: open error\n", body);
        return 1;
      }
      is = &ifs;
    }
  } else {
    is = &std::cin;
  }
  std::string urlstr = url;
  std::ostringstream oss;
  bool isbody = body || meth == kt::HTTPClient::MPOST || meth == kt::HTTPClient::MPUT;
  if (isbody) {
    if (queries->empty()) {
      char c;
      while (is->get(c)) {
        oss.put(c);
      }
    } else {
      std::map<std::string, std::string>::const_iterator it = queries->begin();
      std::map<std::string, std::string>::const_iterator itend = queries->end();
      bool first = true;
      while (it != itend) {
        char* name = kc::urlencode(it->first.data(), it->first.size());
        char* value = kc::urlencode(it->second.data(), it->second.size());
        if (first) {
          first = false;
        } else {
          oss << "&";
        }
        oss << name << "=" << value;
        delete[] value;
        delete[] name;
        it++;
      }
      (*reqheads)["content-type"] = "application/x-www-form-urlencoded";
    }
  } else if (!queries->empty()) {
    std::map<std::string, std::string>::const_iterator it = queries->begin();
    std::map<std::string, std::string>::const_iterator itend = queries->end();
    bool first = !std::strchr(url, '?');
    while (it != itend) {
      char* name = kc::urlencode(it->first.data(), it->first.size());
      char* value = kc::urlencode(it->second.data(), it->second.size());
      if (first) {
        first = false;
        urlstr.append("?");
      } else {
        urlstr.append("&");
      }
      urlstr.append(name);
      urlstr.append("=");
      urlstr.append(value);
      delete[] value;
      delete[] name;
      it++;
    }
  }
  if (!kt::strmapget(*reqheads, "user-agent")) {
    std::string uastr;
    kc::strprintf(&uastr, "KyotoTycoon/%s", kt::VERSION);
    (*reqheads)["user-agent"] = uastr;
  }
  if (!kt::strmapget(*reqheads, "accept")) (*reqheads)["accept"] = "*/*";
  const std::string& ostr = oss.str();
  const std::string* reqbody = isbody ? &ostr : NULL;
  if (meth == kt::HTTPClient::MUNKNOWN)
    meth = isbody ? kt::HTTPClient::MPOST : kt::HTTPClient::MGET;
  bool err = false;
  class Worker : public kc::Thread {
  public:
    Worker() :
      rnum_(0), url_(NULL), meth_(), reqheads_(NULL), reqbody_(NULL), tout_(0),
      err_(false), okcnt_(0) {}
    void setparams(int64_t rnum, const char* url, kt::HTTPClient::Method meth,
                   const std::map<std::string, std::string>* reqheads,
                   const std::string* reqbody, double tout, bool ka) {
      rnum_ = rnum;
      url_ = url;
      meth_ = meth;
      reqheads_ = reqheads;
      reqbody_ = reqbody;
      tout_ = tout;
      ka_ = ka;
    }
    bool error() {
      return err_;
    }
    int64_t okcnt() {
      return okcnt_;
    }
  private:
    void run() {
      kt::URL u(url_);
      kt::HTTPClient ua;
      bool open = false;
      std::map<std::string, std::string> resheads;
      for (int64_t i = 1; i <= rnum_; i++) {
        if (!open) {
          if (!ua.open(u.host(), u.port(), tout_)) {
            err_ = true;
            break;
          }
          open = true;
        }
        int32_t code = ua.fetch(u.path_query(), meth_, NULL, &resheads, reqbody_, reqheads_);
        if (code >= 200 && code < 300) okcnt_++;
        const char* conn = kt::strmapget(resheads, "connection");
        if (!ka_ || code < 0 || (conn && !kc::stricmp(conn, "close"))) {
          if (!ua.close(false)) err_ = true;
          open = false;
        }
      }
      if (open && !ua.close()) err_ = true;
    }
    int64_t rnum_;
    const char* url_;
    kt::HTTPClient::Method meth_;
    const std::map<std::string, std::string>* reqheads_;
    const std::string* reqbody_;
    double tout_;
    bool ka_;
    bool err_;
    int64_t okcnt_;
  };
  Worker* workers = new Worker[thnum];
  double stime = kc::time();
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].setparams(rnum, url, meth, reqheads, reqbody, tout, ka);
    workers[i].start();
  }
  int64_t okcnt = 0;
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].join();
    if (workers[i].error()) err = true;
    okcnt += workers[i].okcnt();
  }
  double etime = kc::time();
  iprintf("OK count: %lld\n", (long long)okcnt);
  iprintf("NG count: %lld\n", (long long)(rnum * thnum - okcnt));
  iprintf("time: %.3f\n", etime - stime);
  iprintf("throughput: %.3f req/seq\n", okcnt / (etime - stime));
  delete[] workers;
  if (!err && okcnt == rnum) iprintf("all of %lld requests were OK\n", rnum);
  return err ? 1 : 0;
}



// END OF FILE
