/*************************************************************************************************
 * The command line interface of miscellaneous utilities
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


// function prototypes
int main(int argc, char** argv);
static void usage();
static int32_t rundate(int32_t argc, char** argv);
static int32_t runhttp(int32_t argc, char** argv);
static int32_t runrpc(int32_t argc, char** argv);
static int32_t procdate(const char* str, int32_t jl, bool wf, bool rf);
static int32_t prochttp(const char* url, kt::HTTPClient::Method meth, const char* body,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries,
                        double tout, bool ph, int32_t ec);
static int32_t procrpc(const char* proc, std::map<std::string, std::string>* params,
                       const char* host, int32_t port, double tout, int32_t ienc, int32_t oenc);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "date")) {
    rv = rundate(argc, argv);
  } else if (!std::strcmp(argv[1], "http")) {
    rv = runhttp(argc, argv);
  } else if (!std::strcmp(argv[1], "rpc")) {
    rv = runrpc(argc, argv);
  } else if (!std::strcmp(argv[1], "version") || !std::strcmp(argv[1], "--version")) {
    printversion();
    rv = 0;
  } else {
    usage();
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: command line interface of miscellaneous utilities of Kyoto Tycoon\n",
          g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s date [-ds str] [-jl num] [-wf] [-rf]\n", g_progname);
  eprintf("  %s http [-get|-head|-post|-put|-delete] [-ah name value] [-qs name value]"
          " [-tout num] [-ph] [-ec num] url [file]\n", g_progname);
  eprintf("  %s rpc [-host str] [-port num] [-tout num] [-ienc str] [-oenc str]"
          " proc [name value ...]\n", g_progname);
  eprintf("  %s version\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// parse arguments of date command
static int32_t rundate(int32_t argc, char** argv) {
  bool argbrk = false;
  const char* str = NULL;
  int32_t jl = INT_MAX;
  bool wf = false;
  bool rf = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-ds")) {
        if (++i >= argc) usage();
        str = argv[i];
      } else if (!std::strcmp(argv[i], "-jl")) {
        if (++i >= argc) usage();
        jl = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-wf")) {
        wf = true;
      } else if (!std::strcmp(argv[i], "-rf")) {
        rf = true;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  int32_t rv = procdate(str, jl, wf, rf);
  return rv;
}


// parse arguments of http command
static int32_t runhttp(int32_t argc, char** argv) {
  bool argbrk = false;
  const char* url = NULL;
  kt::HTTPClient::Method meth = kt::HTTPClient::MUNKNOWN;
  const char* body = NULL;
  std::map<std::string, std::string> reqheads;
  std::map<std::string, std::string> queries;
  double tout = 0;
  bool ph = false;
  int32_t ec = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-ph")) {
        ph = true;
      } else if (!std::strcmp(argv[i], "-ec")) {
        if (++i >= argc) usage();
        ec = kc::atoi(argv[i]);
      } else {
        usage();
      }
    } else if (!url) {
      argbrk = true;
      url = argv[i];
    } else {
      usage();
    }
  }
  if (!url) usage();
  int32_t rv = prochttp(url, meth, body, &reqheads, &queries, tout, ph, ec);
  return rv;
}


// parse arguments of rpc command
static int32_t runrpc(int32_t argc, char** argv) {
  bool argbrk = false;
  const char* proc = NULL;
  std::map<std::string, std::string> params;
  const char* host = NULL;
  int32_t port = kt::DEFPORT;
  double tout = 0;
  int32_t ienc = 0;
  int32_t oenc = 0;
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
      } else if (!std::strcmp(argv[i], "-ienc")) {
        if (++i >= argc) usage();
        ienc = argv[i][0];
      } else if (!std::strcmp(argv[i], "-oenc")) {
        if (++i >= argc) usage();
        oenc = argv[i][0];
      } else {
        usage();
      }
    } else if (!proc) {
      argbrk = true;
      proc = argv[i];
    } else {
      if (++i >= argc) usage();
      params[argv[i-1]] = argv[i];
    }
  }
  if (!proc || port < 1) usage();
  int32_t rv = procrpc(proc, &params, host, port, tout, ienc, oenc);
  return rv;
}


// perform date command
static int32_t procdate(const char* str, int32_t jl, bool wf, bool rf) {
  int64_t t = str ? kt::strmktime(str) : std::time(NULL);
  if (wf) {
    char buf[48];
    kt::datestrwww(t, jl, buf);
    printf("%s\n", buf);
  } else if (rf) {
    char buf[48];
    kt::datestrhttp(t, jl, buf);
    iprintf("%s\n", buf);
  } else {
    iprintf("%lld\n", (long long)t);
  }
  return 0;
}


// perform http command
static int32_t prochttp(const char* url, kt::HTTPClient::Method meth, const char* body,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries,
                        double tout, bool ph, int32_t ec) {
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
        eprintf("%s: %s: open error\n", g_progname, body);
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
  std::string resbody;
  std::map<std::string, std::string> resheads;
  if (meth == kt::HTTPClient::MUNKNOWN)
    meth = isbody ? kt::HTTPClient::MPOST : kt::HTTPClient::MGET;
  bool err = false;
  int32_t code = kt::HTTPClient::fetch_once(urlstr, meth,
                                            &resbody, &resheads, reqbody, reqheads, tout);
  if ((ec < 1 && code > 0) || code == ec) {
    if (ph) {
      std::map<std::string, std::string>::const_iterator it = resheads.begin();
      std::map<std::string, std::string>::const_iterator itend = resheads.end();
      while (it != itend) {
        if (it->first.size() < 1) {
          std::cout << it->second << std::endl;
        } else {
          char* name = kc::strdup(it->first.c_str());
          kt::strcapitalize(name);
          std::cout << name << ": " << it->second << std::endl;
          delete[] name;
        }
        it++;
      }
      std::cout << std::endl;
    }
    std::cout << resbody;
  } else {
    err = true;
    const char* msg = NULL;;
    if (code < 0) {
      msg = resbody.c_str();
    } else {
      msg = kt::strmapget(resheads, "");
    }
    if (!msg) msg = "unknown error";
    eprintf("%s: %s: error: %d: %s\n", g_progname, url, code, msg);
  }
  return err ? 1 : 0;
}


// perform http command
static int32_t procrpc(const char* proc, std::map<std::string, std::string>* params,
                       const char* host, int32_t port, double tout, int32_t ienc, int32_t oenc) {
  std::string lhost = kt::Socket::get_local_host_name();
  if (!host) {
    if (lhost.empty()) {
      eprintf("%s: getting the local host name failed", g_progname);
      return 1;
    }
    host = lhost.c_str();
  }
  bool err = false;
  kt::RPCClient rpc;
  if (!rpc.open(host, port, tout)) {
    eprintf("%s: opening the connection failed\n", g_progname);
    return 1;
  }
  if (ienc != 0) kt::tsvmapdecode(params, ienc);
  std::map<std::string, std::string> outmap;
  kt::RPCClient::ReturnValue rv = rpc.call(proc, params, &outmap);
  if (rv != kt::RPCClient::RVSUCCESS) err = true;
  const char* name;
  switch (rv) {
    case kt::RPCClient::RVSUCCESS: name = "RVSUCCESS"; break;
    case kt::RPCClient::RVEINVALID: name = "RVEINVALID"; break;
    case kt::RPCClient::RVELOGIC: name = "RVELOGIC"; break;
    case kt::RPCClient::RVEINTERNAL: name = "RVEINTERNAL"; break;
    case kt::RPCClient::RVENETWORK: name = "RVENETWORK"; break;
    default: name = "RVEMISC"; break;
  }
  iprintf("RV\t%d: %s\n", (int)rv, name);


  if (oenc != 0) kt::tsvmapencode(&outmap, oenc);

  std::map<std::string, std::string>::iterator it = outmap.begin();
  std::map<std::string, std::string>::iterator itend = outmap.end();
  while (it != itend) {
    iprintf("%s\t%s\n", it->first.c_str(), it->second.c_str());
    it++;
  }
  if (!rpc.close()) {
    eprintf("%s: closing the connection failed\n", g_progname);
    err = true;
  }
  return err ? 1 : 0;
}



// END OF FILE
