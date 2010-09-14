/*************************************************************************************************
 * Network functions
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


#include "ktsocket.h"
#include "myconf.h"

namespace kyototycoon {                  // common namespace


/**
 * Constants for implementation.
 */
namespace {
const int32_t NAMEBUFSIZ = 256;
const int32_t IOBUFSIZ = 4096;
const int32_t DEFPORT = 1978;
const double WAITTIME = 0.1;
const int32_t RECVMAXSIZ = 1 << 30;
}


/**
 * Socket internal.
 */
struct SocketCore {
  const char* errmsg;                    ///< error message
  int32_t fd;                            ///< file descriptor
  std::string expr;                      ///< address expression
  double timeout;                        ///< timeout
  bool aborted;                          ///< flag for abortion
  uint32_t evflags;                      ///< event flags
  char* buf;                             ///< receiving buffer
  const char* rp;                        ///< reading pointer
  const char* ep;                        ///< end pointer
};


/**
 * ServerSocket internal.
 */
struct ServerSocketCore {
  const char* errmsg;                    ///< error message
  int32_t fd;                            ///< file descriptor
  std::string expr;                      ///< address expression
  double timeout;                        ///< timeout
  bool aborted;                          ///< flag for abortion
  uint32_t evflags;                      ///< event flags
};


/**
 * Poller internal.
 */
struct PollerCore {
  const char* errmsg;                    ///< error message
  bool open;                             ///< flag for open
  std::set<Pollable*> items;             ///< polled file descriptors
  std::set<Pollable*> hits;              ///< notified file descriptors
  bool aborted;                          ///< flag for abortion
};


/**
 * Ignore useless signals.
 */
static void ignoresignal();


/**
 * Parse an address expression.
 * @param expr an address expression.
 * @param buf the pointer to the buffer into which the address is written.
 * @param portp the pointer to the variable into which the port is assigned.
 */
static void parseaddr(const char* expr, char* addr, int32_t* portp);


/**
 * Set options of a sockeet.
 * @param fd the file descriptor of the socket.
 * @return true on success, or false on failure.
 */
static bool setsocketoptions(int32_t fd);


/**
 * Clear the error status of a socket.
 * @param fd the file descriptor of the socket.
 */
static void clearsocketerror(int32_t fd);


/**
 * Wait an I/O event of a socket.
 * @param fd the file descriptor of the socket.
 * @param mode the kind of events: 0 for reading, 1 for writing, 2 for exception.
 * @param timeout the timeout in seconds.
 * @return true on success, or false on failure.
 */
static bool waitsocket(int32_t fd, uint32_t mode, double timeout);


/**
 * Set the error message of a socket.
 * @param core the inner condition of the socket.
 * @param msg the error message.
 */
static void sockseterrmsg(SocketCore* core, const char* msg);


/**
 * Receive one byte from a socket.
 * @param core the inner condition of the socket.
 * @return the received byte or -1 on failure.
 */
static int32_t sockgetc(SocketCore* core);


/**
 * Set the error message of a server.
 * @param core the inner condition of the server.
 * @param msg the error message.
 */
static void servseterrmsg(ServerSocketCore* core, const char* msg);


/**
 * Set the error message of a poller.
 * @param core the inner condition of the poller.
 * @param msg the error message.
 */
static void pollseterrmsg(PollerCore* core, const char* msg);


/**
 * Default constructor.
 */
Socket::Socket() {
  _assert_(true);
  SocketCore* core = new SocketCore;
  core->errmsg = NULL;
  core->fd = -1;
  core->timeout = UINT32_MAX;
  core->aborted = false;
  core->buf = NULL;
  core->rp = NULL;
  core->ep = NULL;
  core->evflags = 0;
  opq_ = core;
  ignoresignal();
}


/**
 * Destructor.
 */
Socket::~Socket() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd >= 0) close();
  delete core;
}


/**
 * Get the last happened error information.
 * @return the last happened error information.
 */
const char* Socket::error() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
}


/**
 * Open a client socket.
 */
bool Socket::open(const std::string& expr) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd > 0) {
    sockseterrmsg(core, "already opened");
    return false;
  }
  char addr[NAMEBUFSIZ];
  int32_t port;
  parseaddr(expr.c_str(), addr, &port);
  if (kc::atoi(addr) < 1 || port < 1 || port > INT16_MAX) {
    sockseterrmsg(core, "invalid address expression");
    return false;
  }
  struct ::sockaddr_in sain;
  std::memset(&sain, 0, sizeof(sain));
  sain.sin_family = AF_INET;
  if (::inet_aton(addr, &sain.sin_addr) == 0) {
    sockseterrmsg(core, "inet_aton failed");
    return false;
  }
  uint16_t snum = port;
  sain.sin_port = htons(snum);
  int32_t fd = ::socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    sockseterrmsg(core, "socket failed");
    return false;
  }
  if (!setsocketoptions(fd)) {
    sockseterrmsg(core, "setsocketoptions failed");
    ::close(fd);
    return false;
  }
  int32_t flags = ::fcntl(fd, F_GETFL, NULL);
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    sockseterrmsg(core, "fcntl failed");
    ::close(fd);
    return false;
  }
  double ct = kc::time();
  while (true) {
    if (::connect(fd, (struct sockaddr*)&sain, sizeof(sain)) == 0) break;
    int32_t en = errno;
    if (en != EINTR && en != EAGAIN && en != EINPROGRESS && en != ETIMEDOUT) {
      sockseterrmsg(core, "connect failed");
      ::close(fd);
      return false;
    }
    if (kc::time() > ct + core->timeout) {
      sockseterrmsg(core, "operation timed out");
      ::close(fd);
      return false;
    }
    if (core->aborted) {
      sockseterrmsg(core, "operation was aborted");
      ::close(fd);
      return false;
    }
    if (!waitsocket(fd, 1, WAITTIME)) {
      sockseterrmsg(core, "waitsocket failed");
      ::close(fd);
      return false;
    }
  }
  if (::fcntl(fd, F_SETFL, flags) != 0) {
    sockseterrmsg(core, "fcntl failed");
    ::close(fd);
    return false;
  }
  core->fd = fd;
  core->expr.clear();
  kc::strprintf(&core->expr, "%s:%d", addr, port);
  return true;
}


/**
 * Close the socket.
 */
bool Socket::close() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  int32_t flags = ::fcntl(core->fd, F_GETFL, NULL);
  if (::fcntl(core->fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    sockseterrmsg(core, "fcntl failed");
    err = true;
  }
  double ct = kc::time();
  while (true) {
    int32_t rv = ::shutdown(core->fd, SHUT_RDWR);
    int32_t en = errno;
    if (rv == 0 || en != EAGAIN || en != EWOULDBLOCK) break;
    if (kc::time() > ct + core->timeout) {
      sockseterrmsg(core, "operation timed out");
      err = true;
      break;
    }
    if (core->aborted) break;
    if (!waitsocket(core->fd, 1, WAITTIME)) {
      sockseterrmsg(core, "waitsocket failed");
      break;
    }
  }
  if (::close(core->fd) != 0) {
    sockseterrmsg(core, "close failed");
    err = true;
  }
  core->fd = -1;
  delete[] core->buf;
  core->buf = NULL;
  core->rp = NULL;
  core->ep = NULL;
  core->aborted = false;
  return !err;
}


/**
 * Send data.
 */
bool Socket::send(const void* buf, size_t size) {
  _assert_(buf && size <= kc::MEMMAXSIZ);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  const char* rp = (const char*)buf;
  double ct = kc::time();
  while (size > 0) {
    int32_t wb = ::send(core->fd, rp, size, 0);
    int32_t en = errno;
    switch (wb) {
      case -1: {
        if (en != EINTR && en != EAGAIN && en != EWOULDBLOCK) {
          sockseterrmsg(core, "send failed");
          return false;
        }
        if (kc::time() > ct + core->timeout) {
          sockseterrmsg(core, "operation timed out");
          return false;
        }
        if (core->aborted) {
          sockseterrmsg(core, "operation was aborted");
          return false;
        }
        if (!waitsocket(core->fd, 1, WAITTIME)) {
          sockseterrmsg(core, "waitsocket failed");
          return false;
        }
        break;
      }
      case 0: {
        break;
      }
      default: {
        rp += wb;
        size -= wb;
        break;
      }
    }
  }
  return true;
}


/**
 * Send data.
 */
bool Socket::send(const std::string& str) {
  _assert_(true);
  return send(str.data(), str.size());
}


/**
 * Send formatted data.
 */
bool Socket::printf(const char* format, ...) {
  _assert_(format);
  va_list ap;
  va_start(ap, format);
  bool rv = vprintf(format, ap);
  va_end(ap);
  return rv;
}


/**
 * Send formatted data.
 */
bool Socket::vprintf(const char* format, va_list ap) {
  _assert_(format);
  std::string str;
  kc::vstrprintf(&str, format, ap);
  return send(str);
}


/**
 * Receive data.
 */
bool Socket::receive(void* buf, size_t size) {
  _assert_(buf && size <= kc::MEMMAXSIZ);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  if (core->rp + size <= core->ep) {
    std::memcpy(buf, core->rp, size);
    core->rp += size;
    return true;
  }
  bool err = false;
  char* wp = (char*)buf;
  while (size > 0) {
    int32_t c = sockgetc(core);
    if (c < 0) {
      err = true;
      break;
    }
    *(wp++) = c;
    size--;
  }
  return !err;
}


/**
 * Receive one byte.
 */
int32_t Socket::receive_byte() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  return sockgetc(core);
}


/**
 * Push one byte back.
 */
bool Socket::undo_receive_byte(int32_t c) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  if (core->rp <= core->buf) return false;
  core->rp--;
  *(unsigned char*)core->rp = c;
  return true;
}


/**
 * Receive one line of characters.
 */
bool Socket::receive_line(void* buf, size_t max) {
  _assert_(buf && max > 0 && max <= kc::MEMMAXSIZ);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  char* wp = (char*)buf;
  while (max > 1) {
    int32_t c = sockgetc(core);
    if (c == '\n') break;
    if (c < 0) {
      err = true;
      break;
    }
    if (c != '\r') {
      *(wp++) = c;
      max--;
    }
  }
  *wp = '\0';
  return !err;
}


/**
 * Abort the current operation.
 */
bool Socket::abort() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
}


/**
 * Set the timeout of each operation.
 */
bool Socket::set_timeout(double timeout) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd > 0) {
    sockseterrmsg(core, "already opened");
    return false;
  }
  core->timeout = timeout > 0 ? timeout : UINT32_MAX;
  if (timeout > UINT32_MAX) timeout = UINT32_MAX;
  return true;
}


/**
 * Get the expression of the socket.
 */
const std::string Socket::expression() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 0) {
    sockseterrmsg(core, "not opened");
    return "";
  }
  return core->expr;
}


/**
 * Get the descriptor integer.
 */
int32_t Socket::descriptor() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 0) {
    sockseterrmsg(core, "not opened");
    return -1;
  }
  return core->fd;
}


/**
 * Set event flags.
 */
void Socket::set_event_flags(uint32_t flags) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  core->evflags = flags;
}


/**
 * Get the current event flags.
 */
uint32_t Socket::event_flags() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  return core->evflags;
}


/**
 * Get the primary name of the local host.
 */
std::string Socket::get_local_host_name() {
  _assert_(true);
  char name[NAMEBUFSIZ];
  if (::gethostname(name, sizeof(name) - 1) != 0) return "";
  return name;
}


/**
 * Get the address of a host.
 */
std::string Socket::get_host_address(const std::string& name) {
  _assert_(true);
  struct ::addrinfo hints, *result;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = 0;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_canonname = NULL;
  hints.ai_addr = NULL;
  hints.ai_next = NULL;
  if (::getaddrinfo(name.c_str(), NULL, &hints, &result) != 0) return "";
  if (!result || result->ai_addr->sa_family != AF_INET) {
    ::freeaddrinfo(result);
    return "";
  }
  char addr[NAMEBUFSIZ];
  if (::getnameinfo(result->ai_addr, result->ai_addrlen,
                    addr, sizeof(addr) - 1, NULL, 0, NI_NUMERICHOST) != 0) {
    ::freeaddrinfo(result);
    return "";
  }
  ::freeaddrinfo(result);
  return addr;
}


/**
 * Fetch a resource by HTTP.
 */
int32_t Socket::fetch_http(const std::string& url, std::string* resbody,
                           std::map<std::string, std::string>* resheads,
                           const std::string* reqbody,
                           const std::map<std::string, std::string>* reqheads,
                           double timeout) {
  _assert_(true);
  std::map<std::string, std::string> elems;
  urlbreak(url.c_str(), &elems);
  const char* scheme = strmapget(elems, "scheme");
  if (!scheme || std::strcmp(scheme, "http")) {
    if (resbody) resbody->append("[invalid URL expression]");
    return -1;
  }
  const char* host = strmapget(elems, "host");
  if (!host || *host == '\0') {
    if (resbody) resbody->append("[invalid URL expression]");
    return -1;
  }
  int32_t port = 80;
  const char* portstr = strmapget(elems, "port");
  if (portstr) {
    port = kc::atoi(portstr);
    if (port < 1) {
      if (resbody) resbody->append("[invalid URL expression]");
      return -1;
    }
  }
  const char* path = strmapget(elems, "path");
  if (!path || *path != '/') {
    if (resbody) resbody->append("[invalid URL expression]");
    return -1;
  }
  const char* query = strmapget(elems, "query");
  const char* auth = strmapget(elems, "authority");
  const std::string& addr = get_host_address(host);
  if (addr.empty()) {
    if (resbody) resbody->append("[unknown host]");
    return -1;
  }
  const std::string& expr = kc::strprintf("%s:%d", addr.c_str(), port);
  Socket sock;
  sock.set_timeout(timeout);
  if (!sock.open(expr)) {
    if (resbody) resbody->append("[connection refused]");
    return -1;
  }
  std::string request;
  const char* method = reqheads ? strmapget(*reqheads, "") : NULL;
  if (!method) {
    if (reqbody) {
      method = "POST";
    } else if (resbody) {
      method = "GET";
    } else {
      method = "HEAD";
    }
  }
  char* mstr = kc::strdup(method);
  kc::strnrmspc(mstr);
  kc::strtoupper(mstr);
  kc::strprintf(&request, "%s %s", mstr, path);
  delete[] mstr;
  if (query) kc::strprintf(&request, "?%s", query);
  kc::strprintf(&request, " HTTP/1.1\r\n");
  kc::strprintf(&request, "Host: %s", host);
  if (port != 80) kc::strprintf(&request, ":%d", port);
  kc::strprintf(&request, "\r\n");
  kc::strprintf(&request, "Connection: close\r\n");
  if (auth) {
    char* enc = kc::baseencode(auth, std::strlen(auth));
    kc::strprintf(&request, "Authorization: Basic %s\r\n", enc);
    delete[] enc;
  }
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
      if (*name != '\0' && !std::strchr(name, ':') && !std::strchr(name, ' ') &&
          std::strcmp(name, "host") && std::strcmp(name, "connection") &&
          std::strcmp(name, "content-length")) {
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
  if (!sock.send(request)) {
    if (resbody) resbody->append("[sending data failed]");
    return -1;
  }
  char line[IOBUFSIZ];
  if (!sock.receive_line(line, sizeof(line))) {
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
    if (!sock.receive_line(line, sizeof(line))) {
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
  if (kc::stricmp(method, "HEAD") && code != 304) {
    if (clen >= 0) {
      if (clen > RECVMAXSIZ) {
        if (resbody) resbody->append("[too large response]");
        return -1;
      }
      char* body = new char[clen];
      if (!sock.receive(body, clen)) {
        if (resbody) resbody->append("[receiving data failed]");
        delete[] body;
        return -1;
      }
      if (resbody) resbody->append(body, clen);
      delete[] body;
    } else if (chunked) {
      int64_t asiz = IOBUFSIZ;
      char* body = (char*)kc::xmalloc(asiz);
      int64_t bsiz = 0;
      while (true) {
        if (!sock.receive_line(line, sizeof(line))) {
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
        if (csiz > 0 && !sock.receive(body + bsiz, csiz)) {
          if (resbody) resbody->append("[receiving data failed]");
          kc::xfree(body);
          return -1;
        }
        if (sock.receive_byte() != '\r' || sock.receive_byte() != '\n') {
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
      int64_t asiz = IOBUFSIZ;
      char* body = (char*)kc::xmalloc(asiz);
      int64_t bsiz = 0;
      while (true) {
        int32_t c = sock.receive_byte();
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
  if (!sock.close()) {
    if (resbody) {
      resbody->clear();
      resbody->append("[close failed]");
    }
    return -1;
  }
  return code;
}


/**
 * Default constructor.
 */
ServerSocket::ServerSocket() {
  _assert_(true);
  ServerSocketCore* core = new ServerSocketCore;
  core->errmsg = NULL;
  core->fd = -1;
  core->timeout = UINT32_MAX;
  core->aborted = false;
  core->evflags = 0;
  opq_ = core;
  ignoresignal();
}


/**
 * Destructor.
 */
ServerSocket::~ServerSocket() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd >= 0) close();
  delete core;
}


/**
 * Get the last happened error information.
 */
const char* ServerSocket::error() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
}


/**
 * Open a server socket.
 */
bool ServerSocket::open(const std::string& expr) {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd > 0) {
    servseterrmsg(core, "already opened");
    return false;
  }
  char addr[NAMEBUFSIZ];
  int32_t port;
  parseaddr(expr.c_str(), addr, &port);
  if (*addr == '\0') {
    std::sprintf(addr, "0.0.0.0");
  } else if (kc::atoi(addr) < 1) {
    servseterrmsg(core, "invalid address expression");
    return false;
  }
  if (port < 1 || port > INT16_MAX) {
    servseterrmsg(core, "invalid address expression");
    return false;
  }
  struct ::sockaddr_in sain;
  std::memset(&sain, 0, sizeof(sain));
  sain.sin_family = AF_INET;
  if (::inet_aton(addr, &sain.sin_addr) == 0) {
    servseterrmsg(core, "inet_aton failed");
    return false;
  }
  uint16_t snum = port;
  sain.sin_port = htons(snum);
  int32_t fd = ::socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    servseterrmsg(core, "socket failed");
    return false;
  }
  int32_t optint = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&optint, sizeof(optint));
  if (::bind(fd, (struct sockaddr*)&sain, sizeof(sain)) != 0) {
    servseterrmsg(core, "bind failed");
    ::close(fd);
    return false;
  }
  if (::listen(fd, SOMAXCONN) != 0) {
    servseterrmsg(core, "listen failed");
    ::close(fd);
    return -1;
  }
  int32_t flags = ::fcntl(fd, F_GETFL, NULL);
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    servseterrmsg(core, "fcntl failed");
    ::close(fd);
    return false;
  }
  core->fd = fd;
  core->expr.clear();
  kc::strprintf(&core->expr, "%s:%d", addr, port);
  core->aborted = false;
  return true;
}


/**
 * Close the socket.
 */
bool ServerSocket::close() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 1) {
    servseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  if (::close(core->fd) != 0) {
    servseterrmsg(core, "close failed");
    err = true;
  }
  core->fd = -1;
  core->aborted = false;
  return !err;
}


/**
 * Accept a connection from a client.
 */
bool ServerSocket::accept(Socket* sock) {
  _assert_(sock);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 1) {
    servseterrmsg(core, "not opened");
    return false;
  }
  SocketCore* sockcore = (SocketCore*)sock->opq_;
  if (sockcore->fd >= 0) {
    servseterrmsg(core, "socket was already opened");
    return false;
  }
  double ct = kc::time();
  while (true) {
    struct sockaddr_in sain;
    std::memset(&sain, 0, sizeof(sain));
    sain.sin_family = AF_INET;
    ::socklen_t slen = sizeof(sain);
    int32_t fd = ::accept(core->fd, (struct sockaddr*)&sain, &slen);
    if (fd >= 0) {
      if (!setsocketoptions(fd)) {
        servseterrmsg(core, "setsocketoptions failed");
        ::close(fd);
        return false;
      }
      char addr[NAMEBUFSIZ];
      if (::getnameinfo((struct sockaddr*)&sain, sizeof(sain), addr, sizeof(addr),
                        NULL, 0, NI_NUMERICHOST) != 0) std::sprintf(addr, "0.0.0.0");
      int32_t port = ntohs(sain.sin_port);
      sockcore->fd = fd;
      sockcore->expr.clear();
      kc::strprintf(&sockcore->expr, "%s:%d", addr, port);
      sockcore->aborted = false;
      return true;
    } else {
      int32_t en = errno;
      if (en != EINTR && en != EAGAIN && en != EINPROGRESS && en != ETIMEDOUT) {
        servseterrmsg(core, "accept failed");
        break;
      }
      if (kc::time() > ct + core->timeout) {
        servseterrmsg(core, "operation timed out");
        break;
      }
      if (core->aborted) {
        servseterrmsg(core, "operation was aborted");
        break;
      }
      if (!waitsocket(core->fd, 1, WAITTIME)) {
        servseterrmsg(core, "waitsocket failed");
        break;
      }
    }
  }
  return false;
}


/**
 * Abort the current operation.
 */
bool ServerSocket::abort() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 1) {
    servseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
}


/**
 * Set the timeout of each operation.
 */
bool ServerSocket::set_timeout(double timeout) {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd > 0) {
    servseterrmsg(core, "already opened");
    return false;
  }
  core->timeout = timeout > 0 ? timeout : UINT32_MAX;
  if (timeout > UINT32_MAX) timeout = UINT32_MAX;
  return true;
}


/**
 * Get the expression of the socket.
 */
const std::string ServerSocket::expression() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 0) {
    servseterrmsg(core, "not opened");
    return "";
  }
  return core->expr;
}


/**
 * Get the descriptor integer.
 */
int32_t ServerSocket::descriptor() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 0) {
    servseterrmsg(core, "not opened");
    return -1;
  }
  return core->fd;
}


/**
 * Set event flags.
 */
void ServerSocket::set_event_flags(uint32_t flags) {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  core->evflags = flags;
}


/**
 * Get the current event flags.
 */
uint32_t ServerSocket::event_flags() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  return core->evflags;
}


/**
 * Default constructor.
 */
Poller::Poller() {
  _assert_(true);
  PollerCore* core = new PollerCore;
  core->errmsg = NULL;
  core->open = false;
  core->aborted = false;
  opq_ = core;
  ignoresignal();
}


/**
 * Destructor.
 */
Poller::~Poller() {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->open) close();
  delete core;
}


/**
 * Get the last happened error information.
 */
const char* Poller::error() {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
}


/**
 * Open the poller.
 */
bool Poller::open() {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->open) {
    pollseterrmsg(core, "already opened");
    return false;
  }
  core->open = true;
  return true;
}


/**
 * Close the poller.
 */
bool Poller::close() {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->hits.clear();
  core->items.clear();
  core->open = false;
  core->aborted = false;
  return true;
}


/**
 * Register an I/O event.
 */
bool Poller::push(Pollable* event) {
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  if (!core->items.insert(event).second) {
    pollseterrmsg(core, "duplicated");
    return false;
  }
  return true;
}


/**
 * Fetch and remove a notified I/O event.
 */
Pollable* Poller::pop() {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return NULL;
  }
  if (core->hits.empty()) {
    pollseterrmsg(core, "no event");
    return NULL;
  }
  Pollable* item = *core->hits.begin();
  core->hits.erase(item);
  return item;
}


/**
 * Wait one or more notifying events.
 */
bool Poller::wait(double timeout) {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  if (timeout <= 0) timeout = UINT32_MAX;
  core->hits.clear();
  double ct = kc::time();
  while (true) {
    ::fd_set rset, wset, xset;
    FD_ZERO(&rset);
    FD_ZERO(&wset);
    FD_ZERO(&xset);
    std::map<int32_t, Pollable*> rmap;
    std::map<int32_t, Pollable*> wmap;
    std::map<int32_t, Pollable*> xmap;
    int32_t maxfd = 0;
    std::set<Pollable*>::iterator it = core->items.begin();
    std::set<Pollable*>::iterator itend = core->items.end();
    while (it != itend) {
      Pollable* item = *it;
      int32_t fd = item->descriptor();
      uint32_t flags = item->event_flags();
      if (flags & Pollable::EVINPUT) {
        FD_SET(fd, &rset);
        rmap[fd] = item;
      }
      if (flags & Pollable::EVOUTPUT) {
        FD_SET(fd, &wset);
        wmap[fd] = item;
      }
      if (flags & Pollable::EVEXCEPT) {
        FD_SET(fd, &xset);
        xmap[fd] = item;
      }
      if (fd > maxfd) maxfd = fd;
      it++;
    }
    double integ, fract;
    fract = modf(WAITTIME, &integ);
    struct ::timespec ts;
    ts.tv_sec = integ;
    ts.tv_nsec = fract * 999999000;
    int32_t rv = ::pselect(maxfd + 1, &rset, &wset, &xset, &ts, NULL);
    int32_t en = errno;
    if (rv > 0) {
      if (!rmap.empty()) {
        std::map<int32_t, Pollable*>::iterator mit = rmap.begin();
        std::map<int32_t, Pollable*>::iterator mitend = rmap.end();
        while (mit != mitend) {
          if (FD_ISSET(mit->first, &rset)) {
            Pollable* item = mit->second;
            if (core->hits.insert(item).second) {
              item->set_event_flags(Pollable::EVINPUT);
            } else {
              uint32_t flags = item->event_flags();
              item->set_event_flags(flags | Pollable::EVINPUT);
            }
            core->items.erase(item);
          }
          mit++;
        }
      }
      if (!wmap.empty()) {
        std::map<int32_t, Pollable*>::iterator mit = wmap.begin();
        std::map<int32_t, Pollable*>::iterator mitend = wmap.end();
        while (mit != mitend) {
          if (FD_ISSET(mit->first, &wset)) {
            Pollable* item = mit->second;
            if (core->hits.insert(item).second) {
              item->set_event_flags(Pollable::EVOUTPUT);
            } else {
              uint32_t flags = item->event_flags();
              item->set_event_flags(flags | Pollable::EVOUTPUT);
            }
            core->items.erase(item);
          }
          mit++;
        }
      }
      if (!xmap.empty()) {
        std::map<int32_t, Pollable*>::iterator mit = xmap.begin();
        std::map<int32_t, Pollable*>::iterator mitend = xmap.end();
        while (mit != mitend) {
          if (FD_ISSET(mit->first, &xset)) {
            Pollable* item = mit->second;
            if (core->hits.insert(item).second) {
              item->set_event_flags(Pollable::EVEXCEPT);
            } else {
              uint32_t flags = item->event_flags();
              item->set_event_flags(flags | Pollable::EVEXCEPT);
            }
            core->items.erase(item);
          }
          mit++;
        }
      }
      return true;
    } else if (rv == 0 || en == EINTR || en == EAGAIN || en == EWOULDBLOCK) {
      if (kc::time() > ct + timeout) {
        pollseterrmsg(core, "operation timed out");
        break;
      }
      if (core->aborted) {
        pollseterrmsg(core, "operation was aborted");
        break;
      }
    } else {
      pollseterrmsg(core, "pselect failed");
      break;
    }
  }
  return false;
}


/**
 * Notify all registered events.
 */
bool Poller::flush() {
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->hits.clear();
  std::set<Pollable*>::iterator it = core->items.begin();
  std::set<Pollable*>::iterator itend = core->items.end();
  while (it != itend) {
    Pollable* item = *it;
    item->set_event_flags(0);
    core->hits.insert(item);
    it++;
  }
  return true;
}


/**
 * Get the number of events to watch.
 */
int64_t Poller::count() {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return -1;
  }
  return core->items.size();
}


/**
 * Abort the current operation.
 */
bool Poller::abort() {
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
}


/**
 * Ignore useless signals.
 */
static void ignoresignal() {
  static bool first = true;
  if (first) {
    std::signal(SIGPIPE, SIG_IGN);
    first = false;
  }
}


/**
 * Parse an address expression.
 */
static void parseaddr(const char* expr, char* addr, int32_t* portp) {
  _assert_(expr && addr && portp);
  while (*expr > '\0' && *expr <= ' ') {
    expr++;
  }
  const char* pv = std::strchr(expr, ':');
  if (pv) {
    size_t len = pv - expr;
    if (len > NAMEBUFSIZ - 1) len = NAMEBUFSIZ - 1;
    std::memcpy(addr, expr, len);
    addr[len] = '\0';
    *portp = kc::atoi(pv + 1);
  } else {
    size_t len = std::strlen(expr);
    if (len > NAMEBUFSIZ - 1) len = NAMEBUFSIZ - 1;
    std::memcpy(addr, expr, len);
    addr[len] = '\0';
    *portp = DEFPORT;
  }
}


/**
 * Set options of a sockeet.
 */
static bool setsocketoptions(int32_t fd) {
  _assert_(fd >= 0);
  bool err = false;
  double integ;
  double fract = std::modf(WAITTIME, &integ);
  struct ::timeval opttv;
  opttv.tv_sec = (time_t)integ;
  opttv.tv_usec = (long)(fract * 999999);
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char*)&opttv, sizeof(opttv)) != 0)
    err = true;
  opttv.tv_sec = (time_t)integ;
  opttv.tv_usec = (long)(fract * 999999);
  if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char*)&opttv, sizeof(opttv)) != 0)
    err = true;
  int32_t optint = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&optint, sizeof(optint)) != 0)
    err = true;
  optint = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&optint, sizeof(optint)) != 0)
    err = true;
  return !err;
}


/**
 * Clear the error status of a socket.
 */
static void clearsocketerror(int32_t fd) {
  _assert_(fd >= 0);
  int32_t optint = 1;
  ::socklen_t len = sizeof(optint);
  ::getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*)&optint, &len);
}


/**
 * Wait an I/O event of a socket.
 */
static bool waitsocket(int32_t fd, uint32_t mode, double timeout) {
  _assert_(fd >= 0);
  ::fd_set set;
  FD_ZERO(&set);
  FD_SET(fd, &set);
  double integ, fract;
  fract = modf(timeout, &integ);
  struct ::timespec ts;
  ts.tv_sec = integ;
  ts.tv_nsec = fract * 999999000;
  int32_t rv = -1;
  switch (mode) {
    case 0: {
      rv = ::pselect(fd + 1, &set, NULL, NULL, &ts, NULL);
      break;
    }
    case 1: {
      rv = ::pselect(fd + 1, NULL, &set, NULL, &ts, NULL);
      break;
    }
    case 2: {
      rv = ::pselect(fd + 1, NULL, NULL, &set, &ts, NULL);
      break;
    }
  }
  int32_t en = errno;
  if (rv >= 0 || en == EINTR || en == EAGAIN || en == EWOULDBLOCK) {
    clearsocketerror(fd);
    return true;
  }
  clearsocketerror(fd);
  return false;
}


/**
 * Set the error message of a socket.
 */
static void sockseterrmsg(SocketCore* core, const char* msg) {
  _assert_(core && msg);
  core->errmsg = msg;
}


/**
 * Receive one byte from a socket.
 */
static int32_t sockgetc(SocketCore* core) {
  _assert_(core);
  if (core->rp < core->ep) return *(unsigned char*)(core->rp++);
  if (!core->buf) {
    core->buf = new char[IOBUFSIZ];
    core->rp = core->buf;
    core->ep = core->buf;
  }
  double ct = kc::time();
  while (true) {
    int32_t rv = ::recv(core->fd, core->buf, IOBUFSIZ, 0);
    int32_t en = errno;
    if (rv > 0) {
      core->rp = core->buf + 1;
      core->ep = core->buf + rv;
      return *(unsigned char*)core->buf;
    } else if (rv == 0) {
      sockseterrmsg(core, "end of stream");
      return -1;
    }
    if (en != EINTR && en != EAGAIN && en != EINPROGRESS && en != ETIMEDOUT) break;
    if (kc::time() > ct + core->timeout) {
      sockseterrmsg(core, "operation timed out");
      return -1;
    }
    if (core->aborted) {
      sockseterrmsg(core, "operation was aborted");
      return -1;
    }
    if (!waitsocket(core->fd, 0, WAITTIME)) {
      sockseterrmsg(core, "waitsocket failed");
      return -1;
    }
  }
  sockseterrmsg(core, "recv failed");
  return -1;
}


/**
 * Set the error message of a server.
 */
static void servseterrmsg(ServerSocketCore* core, const char* msg) {
  _assert_(core && msg);
  core->errmsg = msg;
}


/**
 * Set the error message of a poller.
 */
static void pollseterrmsg(PollerCore* core, const char* msg) {
  _assert_(core && msg);
  core->errmsg = msg;
}


}                                        // common namespace

// END OF FILE
