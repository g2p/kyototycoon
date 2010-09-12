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

enum HTTPMode {                          // HTTP method mode
  HTTPAUTO,                              // auto determination
  HTTPGET,                               // GET
  HTTPHEAD,                              // HEAD
  HTTPPOST,                              // POST
  HTTPPUT,                               // PUT
  HTTPDELETE                             // DELETE
};


// global variables
const char* g_progname;                  // program name


// function prototypes
int main(int argc, char** argv);
static void usage();
static int runhttp(int argc, char** argv);
static int32_t prochttp(const char* url, const char* file, HTTPMode mode,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries,
                        double tout, bool ph, int32_t ec);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "http")) {
    rv = runhttp(argc, argv);
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
  eprintf("  %s http [-get|-head|-post|-put|-delete] [-ah name value] [-qs name value]"
          " [-tout num] [-ph] [-ec num] url [file]\n", g_progname);
  eprintf("  %s version\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// parse arguments of http command
static int runhttp(int argc, char** argv) {
  const char* url = NULL;
  const char* file = NULL;
  HTTPMode mode = HTTPAUTO;
  std::map<std::string, std::string> reqheads;
  std::map<std::string, std::string> queries;
  double tout = 0;
  bool ph = false;
  int32_t ec = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-get")) {
        mode = HTTPGET;
      } else if (!std::strcmp(argv[i], "-head")) {
        mode = HTTPHEAD;
      } else if (!std::strcmp(argv[i], "-post")) {
        mode = HTTPPOST;
      } else if (!std::strcmp(argv[i], "-put")) {
        mode = HTTPPUT;
      } else if (!std::strcmp(argv[i], "-delete")) {
        mode = HTTPDELETE;
      } else if (!std::strcmp(argv[i], "-ah")) {
        if ((i += 2) >= argc) usage();
        reqheads[argv[i-1]] = argv[i];
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
      url = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!url) usage();
  int32_t rv = prochttp(url, file, mode, &reqheads, &queries, tout, ph, ec);
  return rv;
}


// perform http command
static int32_t prochttp(const char* url, const char* file, HTTPMode mode,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries,
                        double tout, bool ph, int32_t ec) {
  const char* istr = file && *file == '@' ? file + 1 : NULL;
  std::istream *is;
  std::ifstream ifs;
  std::istringstream iss(istr ? istr : "");
  if (file) {
    if (istr) {
      is = &iss;
    } else {
      ifs.open(file, std::ios_base::in | std::ios_base::binary);
      if (!ifs) {
        eprintf("%s: %s: open error\n", g_progname, file);
        return 1;
      }
      is = &ifs;
    }
  } else {
    is = &std::cin;
  }
  std::string urlstr = url;
  std::ostringstream oss;
  bool body = file || mode == HTTPPOST || mode == HTTPPUT;
  if (body) {
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
  const std::string& ostr = oss.str();
  const std::string* reqbody = body ? &ostr : NULL;
  std::string resbody;
  std::map<std::string, std::string> resheads;
  const char* method;
  switch (mode) {
    default: method = NULL; break;
    case HTTPGET: method = "GET"; break;
    case HTTPHEAD: method = "HEAD"; break;
    case HTTPPOST: method = "POST"; break;
    case HTTPPUT: method = "PUT"; break;
    case HTTPDELETE: method = "DELETE"; break;
  }
  if (method) (*reqheads)[""] = method;
  bool err = false;
  int32_t code = kt::Socket::fetch_http(urlstr, &resbody, &resheads, reqbody, reqheads, tout);
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



// END OF FILE
