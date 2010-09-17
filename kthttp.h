/*************************************************************************************************
 * HTTP utilities
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


#ifndef _KTHTTP_H                        // duplication check
#define _KTHTTP_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktsocket.h>
#include <ktthserv.h>

namespace kyototycoon {                  // common namespace


/**
 * Constants for implementation.
 */
namespace {
const int32_t LINEBUFSIZ = 8192;
const int32_t RECVMAXSIZ = 1 << 30;
}


/**
 * URL accessor.
 */
class URL {
public:
  /**
   * Default constructor.
   */
  explicit URL() :
    scheme_(""), host_(""), port_(0), authority_(""),
    path_(""), query_(""), fragment_("") {
    _assert_(true);
  }
  /**
   * Constructor.
   * @param expr the string expression of the URL.
   */
  explicit URL(const std::string& expr) :
    scheme_(""), host_(""), port_(0), authority_(""),
    path_(""), query_(""), fragment_("") {
    _assert_(true);
    parse_expression(expr);
  }
  /**
   * Copy constructor.
   * @param src the source object.
   */
  explicit URL(const URL& src) :
    scheme_(src.scheme_), host_(src.host_), port_(src.port_), authority_(src.authority_),
    path_(src.path_), query_(src.query_), fragment_(src.fragment_) {
    _assert_(true);
  }
  /**
   * Destructor
   */
  ~URL() {
    _assert_(true);
  }
  /**
   * Set the string expression of the URL.
   */
  void set_expression(const std::string& expr) {
    _assert_(true);
    parse_expression(expr);
  }
  /**
   * Set the scheme.
   */
  void set_scheme(const std::string& scheme) {
    _assert_(true);
    scheme_ = scheme;
  }
  /**
   * Set the host name.
   */
  void set_host(const std::string& host) {
    _assert_(true);
    host_ = host;
  }
  /**
   * Set the port number.
   */
  void set_port(uint32_t port) {
    _assert_(true);
    port_ = port;
  }
  /**
   * Set the authority information.
   */
  void set_authority(const std::string& authority) {
    _assert_(true);
    authority_ = authority;
  }
  /**
   * Set the path.
   */
  void set_path(const std::string& path) {
    _assert_(true);
    path_ = path;
  }
  /**
   * Set the query string.
   */
  void set_query(const std::string& query) {
    _assert_(true);
    query_ = query;
  }
  /**
   * Set the fragment string.
   */
  void set_fragment(const std::string& fragment) {
    _assert_(true);
    fragment_ = fragment;
  }
  /**
   * Get the string expression of the URL.
   * @return the string expression of the URL.
   */
  std::string expression() {
    _assert_(true);
    std::string expr;
    if (!scheme_.empty()) {
      kc::strprintf(&expr, "%s://", scheme_.c_str());
      if (!authority_.empty()) kc::strprintf(&expr, "%s@", authority_.c_str());
      if (!host_.empty()) {
        kc::strprintf(&expr, "%s", host_.c_str());
        if (port_ > 0 && port_ != default_port(scheme_)) kc::strprintf(&expr, ":%u", port_);
      }
    }
    kc::strprintf(&expr, "%s", path_.c_str());
    if (!query_.empty()) kc::strprintf(&expr, "?%s", query_.c_str());
    if (!fragment_.empty()) kc::strprintf(&expr, "#%s", fragment_.c_str());
    return expr;
  }
  /**
   * Get the path and the query string for HTTP request.
   * @return the path and the query string for HTTP request.
   */
  std::string path_query() {
    _assert_(true);
    return path_ + (query_.empty() ? "" : "?") + query_;
  }
  /**
   * Get the scheme.
   * @return the scheme.
   */
  std::string scheme() {
    _assert_(true);
    return scheme_;
  }
  /**
   * Get the host name.
   * @return the host name.
   */
  std::string host() {
    _assert_(true);
    return host_;
  }
  /**
   * Get the port number.
   * @return the port number.
   */
  uint32_t port() {
    _assert_(true);
    return port_;
  }
  /**
   * Get the authority information.
   * @return the authority information.
   */
  std::string authority() {
    _assert_(true);
    return authority_;
  }
  /**
   * Get the path.
   * @return the path.
   */
  std::string path() {
    _assert_(true);
    return path_;
  }
  /**
   * Get the query string.
   * @return the query string.
   */
  std::string query() {
    _assert_(true);
    return query_;
  }
  /**
   * Get the fragment string.
   * @return the fragment string.
   */
  std::string fragment() {
    _assert_(true);
    return fragment_;
  }
  /**
   * Assignment operator from the self type.
   * @param right the right operand.
   * @return the reference to itself.
   */
  URL& operator =(const URL& right) {
    _assert_(true);
    if (&right == this) return *this;
    scheme_ = right.scheme_;
    host_ = right.host_;
    port_ = right.port_;
    authority_ = right.authority_;
    path_ = right.path_;
    query_ = right.query_;
    fragment_ = right.fragment_;
    return *this;
  }
private:
  /**
   * Parse a string expression.
   * @param expr the string expression.
   */
  void parse_expression(const std::string& expr) {
    scheme_ = "";
    host_ = "";
    port_ = 0;
    authority_ = "";
    path_ = "";
    query_ = "";
    fragment_ = "";
    char* trim = kc::strdup(expr.c_str());
    kc::strtrim(trim);
    char* rp = trim;
    char* norm = new char[std::strlen(trim)*3+1];
    char* wp = norm;
    while (*rp != '\0') {
      if (*rp > 0x20 && *rp < 0x7f) {
        *(wp++) = *rp;
      } else {
        *(wp++) = '%';
        int32_t num = *(unsigned char*)rp >> 4;
        if (num < 10) {
          *(wp++) = '0' + num;
        } else {
          *(wp++) = 'a' + num - 10;
        }
        num = *rp & 0x0f;
        if (num < 10) {
          *(wp++) = '0' + num;
        } else {
          *(wp++) = 'a' + num - 10;
        }
      }
      rp++;
    }
    *wp = '\0';
    rp = norm;
    if (kc::strifwm(rp, "http://")) {
      scheme_ = "http";
      rp += 7;
    } else if (kc::strifwm(rp, "https://")) {
      scheme_ = "https";
      rp += 8;
    } else if (kc::strifwm(rp, "ftp://")) {
      scheme_ = "ftp";
      rp += 6;
    } else if (kc::strifwm(rp, "sftp://")) {
      scheme_ = "sftp";
      rp += 7;
    } else if (kc::strifwm(rp, "ftps://")) {
      scheme_ = "ftps";
      rp += 7;
    } else if (kc::strifwm(rp, "tftp://")) {
      scheme_ = "tftp";
      rp += 7;
    } else if (kc::strifwm(rp, "ldap://")) {
      scheme_ = "ldap";
      rp += 7;
    } else if (kc::strifwm(rp, "ldaps://")) {
      scheme_ = "ldaps";
      rp += 8;
    } else if (kc::strifwm(rp, "file://")) {
      scheme_ = "file";
      rp += 7;
    }
    char* ep;
    if ((ep = std::strchr(rp, '#')) != NULL) {
      fragment_ = ep + 1;
      *ep = '\0';
    }
    if ((ep = std::strchr(rp, '?')) != NULL) {
      query_ = ep + 1;
      *ep = '\0';
    }
    if (!scheme_.empty()) {
      if ((ep = std::strchr(rp, '/')) != NULL) {
        path_ = ep;
        *ep = '\0';
      } else {
        path_ = "/";
      }
      if ((ep = std::strchr(rp, '@')) != NULL) {
        *ep = '\0';
        if (rp[0] != '\0') authority_ = rp;
        rp = ep + 1;
      }
      if ((ep = std::strchr(rp, ':')) != NULL) {
        if (ep[1] != '\0') port_ = kc::atoi(ep + 1);
        *ep = '\0';
      }
      if (rp[0] != '\0') host_ = rp;
      if (port_ < 1) port_ = default_port(scheme_);
    } else {
      path_ = rp;
    }
    delete[] norm;
    delete[] trim;
  }
  /**
   * Get the default port of a scheme.
   * @param scheme the scheme.
   */
  uint32_t default_port(const std::string& scheme) {
    if (scheme == "http") return 80;
    if (scheme == "https") return 443;
    if (scheme == "ftp") return 21;
    if (scheme == "sftp") return 22;
    if (scheme == "ftps") return 990;
    if (scheme == "tftp") return 69;
    if (scheme == "ldap") return 389;
    if (scheme == "ldaps") return 636;
    return 0;
  }
  /** The scheme. */
  std::string scheme_;
  /** The host name. */
  std::string host_;
  /** The port number. */
  uint32_t port_;
  /** The autority information. */
  std::string authority_;
  /** The path. */
  std::string path_;
  /** The query string. */
  std::string query_;
  /** The fragment string. */
  std::string fragment_;
};


/**
 * HTTP client.
 */
class HTTPClient {
public:
  /**
   * Kinds of HTTP request methods.
   */
  enum Method {
    MGET,                                ///< GET
    MHEAD,                               ///< HEAD
    MPOST,                               ///< POST
    MPUT,                                ///< PUT
    MDELETE,                             ///< DELETE
    MUNKNOWN                             ///< unknown
  };
  /**
   * Default constructor.
   */
  HTTPClient() : sock_(), host_(""), port_(0) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~HTTPClient() {
    _assert_(true);
  }
  /**
   * Open the connection.
   * @param host the name or the address of the server.
   * @param port the port numger of the server.
   * @param timeout the timeout of each operation in seconds.  If it is not more than 0, no
   * timeout is specified.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& host, uint32_t port = 80, double timeout = -1) {
    _assert_(true);
    const std::string& addr = Socket::get_host_address(host);
    if (addr.empty()) return false;
    std::string expr;
    kc::strprintf(&expr, "%s:%u", addr.c_str(), port);
    sock_.set_timeout(timeout);
    if (!sock_.open(expr)) return false;
    host_ = host;
    port_ = port;
    return true;
  }
  /**
   * Close the connection.
   * @return true on success, or false on failure.
   */
  bool close() {
    _assert_(true);
    return sock_.close();
  }
  /**
   * Fetch a resource.
   * @param pathquery the path and the query string of the resource.
   * @param method the kind of the request methods.
   * @param resbody a string to contain the entity body of the response.  If it is NULL, it is
   * ignored.
   * @param resheads a string map to contain the headers of the response.  If it is NULL, it is
   * ignored.
   * @param reqbody a string which contains the entity body of the request.  If it is NULL, it
   * is ignored.
   * @param reqheads a string map which contains the headers of the request.  If it is NULL, it
   * is ignored.
   * @return the status code of the response, or -1 on failure.
   */
  int32_t fetch(const std::string& pathquery, Method method = MGET,
                std::string* resbody = NULL,
                std::map<std::string, std::string>* resheads = NULL,
                const std::string* reqbody = NULL,
                const std::map<std::string, std::string>* reqheads = NULL) {
    _assert_(true);
    if (resbody) resbody->clear();
    if (resheads) resheads->clear();
    if (pathquery.empty() || pathquery[0] != '/') {
      if (resbody) resbody->append("[invalid URL expression]");
      return -1;
    }
    std::string request;
    const char* mstr;
    switch (method) {
      default: mstr = "GET"; break;
      case MHEAD: mstr = "HEAD"; break;
      case MPOST: mstr = "POST"; break;
      case MPUT: mstr = "PUT"; break;
      case MDELETE: mstr = "DELETE"; break;
    }
    kc::strprintf(&request, "%s %s HTTP/1.1\r\n", mstr, pathquery.c_str());
    kc::strprintf(&request, "Host: %s", host_.c_str());
    if (port_ != 80) kc::strprintf(&request, ":%u", port_);
    kc::strprintf(&request, "\r\n");
    if (reqbody) kc::strprintf(&request, "Content-Length: %lld\r\n", (long long)reqbody->size());
    if (reqheads) {
      std::map<std::string, std::string>::const_iterator it = reqheads->begin();
      std::map<std::string, std::string>::const_iterator itend = reqheads->end();
      while (it != itend) {
        char* name = kc::strdup(it->first.c_str());
        char* value = kc::strdup(it->second.c_str());
        kc::strnrmspc(name);
        kc::strtolower(name);
        kc::strnrmspc(value);
        if (*name != '\0' && !std::strchr(name, ':') && !std::strchr(name, ' ')) {
          strcapitalize(name);
          kc::strprintf(&request, "%s: %s\r\n", name, value);
        }
        delete[] value;
        delete[] name;
        it++;
      }
    }
    kc::strprintf(&request, "\r\n");
    if (reqbody) request.append(*reqbody);
    if (!sock_.send(request)) {
      if (resbody) resbody->append("[sending data failed]");
      return -1;
    }
    char line[LINEBUFSIZ];
    if (!sock_.receive_line(line, sizeof(line))) {
      if (resbody) resbody->append("[receiving data failed]");
      return -1;
    }
    if (!kc::strfwm(line, "HTTP/1.1 ") && !kc::strfwm(line, "HTTP/1.0 ")) {
      if (resbody) resbody->append("[received data was invalid]");
      return -1;
    }
    const char* rp = line + 9;
    int32_t code = kc::atoi(rp);
    if (code < 1) {
      if (resbody) resbody->append("[invalid status code]");
      return -1;
    }
    if (resheads) (*resheads)[""] = line;
    int64_t clen = -1;
    bool chunked = false;
    while (true) {
      if (!sock_.receive_line(line, sizeof(line))) {
        resbody->append("[receiving data failed]");
        return -1;
      }
      if (*line == '\0') break;
      char* pv = std::strchr(line, ':');
      if (pv) {
        *(pv++) = '\0';
        kc::strnrmspc(line);
        kc::strtolower(line);
        if (*line != '\0') {
          while (*pv == ' ') {
            pv++;
          }
          if (!std::strcmp(line, "content-length")) {
            clen = kc::atoi(pv);
          } else if (!std::strcmp(line, "transfer-encoding")) {
            if (!kc::stricmp(pv, "chunked")) chunked = true;
          }
          if (resheads) (*resheads)[line] = pv;
        }
      }
    }
    if (method != MHEAD && code != 304) {
      if (clen >= 0) {
        if (clen > RECVMAXSIZ) {
          if (resbody) resbody->append("[too large response]");
          return -1;
        }
        char* body = new char[clen];
        if (!sock_.receive(body, clen)) {
          if (resbody) resbody->append("[receiving data failed]");
          delete[] body;
          return -1;
        }
        if (resbody) resbody->append(body, clen);
        delete[] body;
      } else if (chunked) {
        int64_t asiz = LINEBUFSIZ;
        char* body = (char*)kc::xmalloc(asiz);
        int64_t bsiz = 0;
        while (true) {
          if (!sock_.receive_line(line, sizeof(line))) {
            if (resbody) resbody->append("[receiving data failed]");
            kc::xfree(body);
            return -1;
          }
          if (*line == '\0') break;
          int64_t csiz = kc::atoih(line);
          if (bsiz + csiz > RECVMAXSIZ) {
            if (resbody) resbody->append("[too large response]");
            kc::xfree(body);
            return -1;
          }
          if (bsiz + csiz > asiz) {
            asiz = bsiz * 2 + csiz;
            body = (char*)kc::xrealloc(body, asiz);
          }
          if (csiz > 0 && !sock_.receive(body + bsiz, csiz)) {
            if (resbody) resbody->append("[receiving data failed]");
            kc::xfree(body);
            return -1;
          }
          if (sock_.receive_byte() != '\r' || sock_.receive_byte() != '\n') {
            if (resbody) resbody->append("[invalid chunk]");
            kc::xfree(body);
            return -1;
          }
          if (csiz < 1) break;
          bsiz += csiz;
        }
        if (resbody) resbody->append(body, bsiz);
        kc::xfree(body);
      } else {
        int64_t asiz = LINEBUFSIZ;
        char* body = (char*)kc::xmalloc(asiz);
        int64_t bsiz = 0;
        while (true) {
          int32_t c = sock_.receive_byte();
          if (c < 0) break;
          if (bsiz > RECVMAXSIZ - 1) {
            if (resbody) resbody->append("[too large response]");
            kc::xfree(body);
            return -1;
          }
          if (bsiz + 1 > asiz) {
            asiz = bsiz * 2;
            body = (char*)kc::xrealloc(body, asiz);
          }
          body[bsiz++] = c;
        }
        if (resbody) resbody->append(body, bsiz);
        kc::xfree(body);
      }
    }
    return code;
  }
  /**
   * Fetch a resource at once
   * @param url the URL of the resource.
   * @param method the kind of the request methods.
   * @param resbody a string to contain the entity body of the response.  If it is NULL, it is
   * ignored.
   * @param resheads a string map to contain the headers of the response.  If it is NULL, it is
   * ignored.
   * @param reqbody a string which contains the entity body of the request.  If it is NULL, it
   * is ignored.
   * @param reqheads a string map which contains the headers of the request.  If it is NULL, it
   * is ignored.
   * @param timeout the timeout of each operation in seconds.  If it is not more than 0, no
   * timeout is specified.
   * @return the status code of the response, or -1 on failure.
   */
  static int32_t fetch_once(const std::string& url, Method method = MGET,
                            std::string* resbody = NULL,
                            std::map<std::string, std::string>* resheads = NULL,
                            const std::string* reqbody = NULL,
                            const std::map<std::string, std::string>* reqheads = NULL,
                            double timeout = -1) {
    _assert_(true);
    URL uo(url);
    const std::string& scheme = uo.scheme();
    const std::string& host = uo.host();
    uint32_t port = uo.port();
    if (scheme != "http" || host.empty() || port < 1) {
      if (resbody) resbody->append("[invalid URL expression]");
      return -1;
    }
    HTTPClient ua;
    if (!ua.open(host, port, timeout)) {
      if (resbody) resbody->append("[connection refused]");
      return -1;
    }
    std::map<std::string, std::string> rhtmp;
    if (reqheads) rhtmp.insert(reqheads->begin(), reqheads->end());
    rhtmp["connection"] = "close";
    int32_t code = ua.fetch(uo.path_query(), method, resbody, resheads, reqbody, &rhtmp);
    if (!ua.close()) {
      if (resbody) {
        resbody->clear();
        resbody->append("[close failed]");
      }
      return -1;
    }
    return code;
  }
private:
  /** Dummy constructor to forbid the use. */
  HTTPClient(const HTTPClient&);
  /** Dummy Operator to forbid the use. */
  HTTPClient& operator =(const HTTPClient&);
  /** The socket of connection. */
  Socket sock_;
  /** The host name of the server. */
  std::string host_;
  /** The port number of the server. */
  uint32_t port_;
};


/**
 * HTTP server.
 */
class HTTPServer {
public:
  class Worker;
  class Logger;
private:
  class WorkerAdapter;
public:
  /**
   * Interface to process each request.
   */
  class Worker {
  public:
    /**
     * Destructor.
     */
    virtual ~Worker() {}
    /**
     * Process each request.
     * @param pathquery the path and the query string of the resource.
     * @param method the kind of the request methods.
     * @param reqheads a string map which contains the headers of the request.
     * @param reqbody a string which contains the entity body of the request.
     * @param resheads a string map to contain the headers of the response.
     * @param resbody a string to contain the entity body of the response.
     * @param sessdata a string map to contain the session local data.
     * @return the status code of the response.  If it is less than 1, internal server error is
     * sent to the client and the connection is closed.
     */
    virtual int32_t process(const std::string& pathquery, HTTPClient::Method method,
                            const std::map<std::string, std::string>& reqheads,
                            const std::string& reqbody,
                            std::map<std::string, std::string>& resheads,
                            std::string& resbody,
                            std::map<std::string, std::string>& sessdata) = 0;
  };
  /**
   * Interface to log internal information and errors.
   */
  class Logger : public ThreadedServer::Logger {
  public:
    /**
     * Destructor.
     */
    virtual ~Logger() {}
  };
  /**
   * Default constructor.
   */
  HTTPServer() : serv_(), worker_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~HTTPServer() {
    _assert_(true);
  }
  /**
   * Set the network configurations.
   * @param expr an expression of the address and the port of the server.
   * @param timeout the timeout of each network operation in seconds.  If it is not more than 0,
   * no timeout is specified.
   */
  void set_network(const std::string& expr, double timeout = -1) {
    _assert_(true);
    serv_.set_network(expr, timeout);
  }
  /**
   * Set the worker to process each request.
   * @param worker the worker object.
   * @param thnum the number of worker threads.
   */
  void set_worker(Worker* worker, size_t thnum = 1) {
    _assert_(true);
    worker_.worker_ = worker;
    serv_.set_worker(&worker_, thnum);
  }
  /**
   * Set the logger to process each log message.
   * @param logger the logger object.
   * @param kinds kinds of logged messages by bitwise-or: Logger::DEBUG for debugging,
   * Logger::INFO for normal information, Logger::SYSTEM for system information, and
   * Logger::ERROR for fatal error.
   */
  void set_logger(Logger* logger, uint32_t kinds = Logger::SYSTEM | Logger::ERROR) {
    _assert_(true);
    serv_.set_logger(logger, kinds);
  }
  /**
   * Start the service.
   * @return true on success, or false on failure.
   * @note This function blocks until the server stops by the ThreadedServer::stop method.
   */
  bool start() {
    _assert_(true);
    return serv_.start();
  }
  /**
   * Stop the service.
   * @return true on success, or false on failure.
   */
  bool stop() {
    _assert_(true);
    return serv_.stop();
  }
  /**
   * Finish the service.
   * @return true on success, or false on failure.
   */
  bool finish() {
    _assert_(true);
    return serv_.finish();
  }
private:
  /**
   * Adapter for the worker.
   */
  class WorkerAdapter : public ThreadedServer::Worker {
    friend class HTTPServer;
  public:
    WorkerAdapter() : worker_(NULL) {
      _assert_(true);
    }
  private:
    bool process(ThreadedServer::Session* sess) {
      _assert_(true);
      char line[LINEBUFSIZ];
      if (!sess->receive_line(&line, sizeof(line))) return false;
      HTTPClient::Method method;
      std::map<std::string, std::string> reqheads;
      reqheads[""] = line;
      char* pv = std::strchr(line, ' ');
      if (!pv) return false;
      *(pv++) = '\0';
      char* pathquery = pv;
      pv = std::strchr(pathquery, ' ');
      if (!pv) return false;
      *(pv++) = '\0';
      int32_t htver;
      if (!std::strcmp(pv, "HTTP/1.0")) {
        htver = 0;
      } else if (!std::strcmp(pv, "HTTP/1.1")) {
        htver = 1;
      } else {
        return false;
      }
      if (!std::strcmp(line, "GET")) {
        method = HTTPClient::MGET;
      } else if (!std::strcmp(line, "HEAD")) {
        method = HTTPClient::MHEAD;
      } else if (!std::strcmp(line, "POST")) {
        method = HTTPClient::MPOST;
      } else if (!std::strcmp(line, "PUT")) {
        method = HTTPClient::MPUT;
      } else if (!std::strcmp(line, "DELETE")) {
        method = HTTPClient::MDELETE;
      } else {
        method = HTTPClient::MUNKNOWN;
      }
      bool keep = htver >= 1;
      int64_t clen = -1;
      bool chunked = false;
      while (true) {
        if (!sess->receive_line(&line, sizeof(line))) return false;
        if (*line == '\0') break;
        pv = std::strchr(line, ':');
        if (pv) {
          *(pv++) = '\0';
          kc::strnrmspc(line);
          kc::strtolower(line);
          if (*line != '\0') {
            while (*pv == ' ') {
              pv++;
            }
            if (!std::strcmp(line, "connection")) {
              if (!kc::stricmp(pv, "close")) {
                keep = false;
              } else if (!kc::stricmp(pv, "keep-alive")) {
                keep = true;
              }
            } else if (!std::strcmp(line, "content-length")) {
              clen = kc::atoi(pv);
            } else if (!std::strcmp(line, "transfer-encoding")) {
              if (!kc::stricmp(pv, "chunked")) chunked = true;
            }
            reqheads[line] = pv;
          }
        }
      }
      std::string reqbody;
      if (method == HTTPClient::MPOST || method == HTTPClient::MPUT ||
          method == HTTPClient::MUNKNOWN) {
        if (clen >= 0) {
          if (clen > RECVMAXSIZ) {
            send_error(sess, 413, "request entity too large");
            return false;
          }
          char* body = new char[clen];
          if (!sess->receive(body, clen)) {
            send_error(sess, 400, "receiving data failed");
            delete[] body;
            return false;
          }
          reqbody.append(body, clen);
          delete[] body;
        } else if (chunked) {
          int64_t asiz = LINEBUFSIZ;
          char* body = (char*)kc::xmalloc(asiz);
          int64_t bsiz = 0;
          while (true) {
            if (!sess->receive_line(line, sizeof(line))) {
              send_error(sess, 400, "receiving data failed");
              kc::xfree(body);
              return false;
            }
            if (*line == '\0') break;
            int64_t csiz = kc::atoih(line);
            if (bsiz + csiz > RECVMAXSIZ) {
              send_error(sess, 413, "request entity too large");
              kc::xfree(body);
              return false;
            }
            if (bsiz + csiz > asiz) {
              asiz = bsiz * 2 + csiz;
              body = (char*)kc::xrealloc(body, asiz);
            }
            if (csiz > 0 && !sess->receive(body + bsiz, csiz)) {
              send_error(sess, 400, "receiving data failed");
              kc::xfree(body);
              return false;
            }
            if (sess->receive_byte() != '\r' || sess->receive_byte() != '\n') {
              send_error(sess, 400, "invalid chunk");
              kc::xfree(body);
              return false;
            }
            if (csiz < 1) break;
            bsiz += csiz;
          }
          reqbody.append(body, bsiz);
          kc::xfree(body);
        }
      }
      std::string resbody;
      std::map<std::string, std::string> resheads;


      struct MapData : public ThreadedServer::Session::Data {
        std::map<std::string, std::string> map;
      };

      MapData* sessdata = (MapData*)sess->data();

      if (!sessdata) {
        sessdata = new MapData;
        sess->set_data(sessdata);
      }

      int32_t code = worker_->process(pathquery, method, reqheads, reqbody, resheads, resbody,
                                      sessdata->map);
      if (code > 0) {


        send_error(sess, code, "love");



      } else {
        send_error(sess, 500, "logic error");
        keep = false;
      }
      return keep;
    }
    void send_error(ThreadedServer::Session* sess, int32_t code, const char* msg) {
      _assert_(sess && code > 0);
      std::string str;
      kc::strprintf(&str, "%d %s (%s)\n", code, status_name(code), msg);
      std::string data;
      kc::strprintf(&data, "HTTP/1.1 %d Bad Request\r\n", code);
      append_server_headers(&data);
      kc::strprintf(&data, "Connection: close\r\n");
      kc::strprintf(&data, "Content-Length: %d\r\n", (int)str.size());
      kc::strprintf(&data, "Content-Type: text/plain\r\n");
      kc::strprintf(&data, "\r\n");
      data.append(str);
      sess->send(data.data(), data.size());
    }
    void append_server_headers(std::string* str) {
      _assert_(str);
      kc::strprintf(str, "Server: KyotoTycoon/%s\r\n", VERSION);
      char buf[48];
      datestrhttp(INT64_MAX, 0, buf);
      kc::strprintf(str, "Date: %s\r\n", buf);
    }
    const char* status_name(int32_t code) {
      _assert_(true);
      switch (code) {
        case 100: return "Continue";
        case 101: return "Switching Protocols";
        case 102: return "Processing";
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 203: return "Non-Authoritative Information";
        case 204: return "No Content";
        case 205: return "Reset Content";
        case 206: return "Partial Content";
        case 207: return "Multi-Status";
        case 300: return "Multiple Choices";
        case 301: return "Moved Permanently";
        case 302: return "Found";
        case 303: return "See Other";
        case 304: return "Not Modified";
        case 305: return "Use Proxy";
        case 307: return "Temporary Redirect";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 402: return "Payment Required";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 406: return "Not Acceptable";
        case 407: return "Proxy Authentication Required";
        case 408: return "Request Timeout";
        case 409: return "Conflict";
        case 410: return "Gone";
        case 411: return "Length Required";
        case 412: return "Precondition Failed";
        case 413: return "Request Entity Too Large";
        case 414: return "Request-URI Too Long";
        case 415: return "Unsupported Media Type";
        case 416: return "Requested Range Not Satisfiable";
        case 417: return "Expectation Failed";
        case 422: return "Unprocessable Entity";
        case 423: return "Locked";
        case 424: return "Failed Dependency";
        case 426: return "Upgrade Required";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        case 504: return "Gateway Timeout";
        case 505: return "HTTP Version Not Supported";
        case 506: return "Variant Also Negotiates";
        case 507: return "Insufficient Storage";
        case 509: return "Bandwidth Limit Exceeded";
        case 510: return "Not Extended";
      }
      if (code < 100) return "Unknown Status";
      if (code < 200) return "Unknown Informational Status";
      if (code < 300) return "Unknown Success Status";
      if (code < 400) return "Unknown Redirection Status";
      if (code < 500) return "Unknown Client Error Status";
      if (code < 600) return "Unknown Server Error Status";
      return "Unknown Status";
    }
    HTTPServer::Worker* worker_;
  };
  /** Dummy constructor to forbid the use. */
  HTTPServer(const HTTPServer&);
  /** Dummy Operator to forbid the use. */
  HTTPServer& operator =(const HTTPServer&);
  /** The internal server. */
  ThreadedServer serv_;
  /** The adapter for worker. */
  WorkerAdapter worker_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
