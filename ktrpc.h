/*************************************************************************************************
 * RPC utilities
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


#ifndef _KTRPC_H                         // duplication check
#define _KTRPC_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktsocket.h>
#include <ktthserv.h>
#include <kthttp.h>

namespace kyototycoon {                  // common namespace


/**
 * Constants for implementation.
 */
namespace {
const char RPCPATHPREFIX[] = "/rpc/";    ///< prefix of the RPC entry
const char RPCFORMMTYPE[] = "application/x-www-form-urlencoded";  //< MIME type of form data
const char RPCTSVMTYPE[] = "text/tab-separated-values";           //< MIME type of TSV
const char RPCTSVMATTR[] = "colenc";     ///< encoding attribute of TSV
}


/**
 * RPC client
 */
class RPCClient {
public:
  /**
   * Return value.
   */
  enum ReturnValue {
    RVSUCCESS,                           ///< success
    RVEINVALID,                          ///< invalid operation
    RVEINTERNAL,                         ///< internal error
    RVELOGIC                             ///< logical inconsistency
  };
  /**
   * Call a remote procedure.
   * @param name the name of the procecude.
   * @param inmap a string map which contains the input of the procedure.  If it is NULL, it is
   * ignored.
   * @param outmap a string map to contain the input parameters.  If it is NULL, it is ignored.
   * @return the return value of the procedure.
   */
  ReturnValue call(const std::string& name,
                   const std::map<std::string, std::string>* inmap = NULL,
                   std::map<std::string, std::string>* outmap = NULL);
};


/**
 * RPC server.
 */
class RPCServer {
public:
  class Logger;
  class Worker;
  class Session;
private:
  class WorkerAdapter;
public:
  /**
   * Interface to log internal information and errors.
   */
  class Logger : public HTTPServer::Logger {
  public:
    /**
     * Destructor.
     */
    virtual ~Logger() {}
  };
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
     * Process each request of RPC.
     * @param serv the server.
     * @param sess the session with the client.
     * @param name the name of the procecude.
     * @param inmap a string map which contains the input of the procedure.
     * @param outmap a string map to contain the input parameters.
     * @return the return value of the procedure.
     */
    virtual RPCClient::ReturnValue process(RPCServer* serv, Session* sess,
                                           const std::string& name,
                                           const std::map<std::string, std::string>& inmap,
                                           std::map<std::string, std::string>& outmap) = 0;
    /**
     * Process each request of the others.
     * @param serv the server.
     * @param sess the session with the client.
     * @param path the path of the requested resource.
     * @param method the kind of the request methods.
     * @param reqheads a string map which contains the headers of the request.  Header names are
     * converted into lower cases.  The empty key means the request-line.
     * @param reqbody a string which contains the entity body of the request.
     * @param resheads a string map to contain the headers of the response.
     * @param resbody a string to contain the entity body of the response.
     * @param misc a string map which contains miscellaneous information.  "url" means the
     * absolute URL.  "query" means the query string of the URL.
     * @return the status code of the response.  If it is less than 1, internal server error is
     * sent to the client and the connection is closed.
     */
    virtual int32_t process(HTTPServer* serv, HTTPServer::Session* sess,
                            const std::string& path, HTTPClient::Method method,
                            const std::map<std::string, std::string>& reqheads,
                            const std::string& reqbody,
                            std::map<std::string, std::string>& resheads,
                            std::string& resbody,
                            const std::map<std::string, std::string>& misc) {
      _assert_(serv && sess);
      return 501;
    }
  };
  /**
   * Interface to log internal information and errors.
   */
  class Session {
    friend class RPCServer;
  public:
    /**
     * Interface of session local data.
     */
    class Data  : public HTTPServer::Session::Data {
    public:
      /**
       * Destructor.
       */
      virtual ~Data() {
        _assert_(true);
      }
    };
    /**
     * Get the ID number of the session.
     * @return the ID number of the session.
     */
    uint64_t id() {
      _assert_(true);
      return sess_->id();
    }
    /**
     * Get the ID number of the worker thread.
     * @return the ID number of the worker thread.  It is from 0 to less than the number of
     * worker threads.
     */
    uint32_t thread_id() {
      _assert_(true);
      return sess_->thread_id();
    }
    /**
     * Set the session local data.
     * @param data the session local data.  If it is NULL, no data is registered.
     * @note The registered data is destroyed implicitly when the session object is destroyed or
     * this method is called again.
     */
    void set_data(Data* data) {
      _assert_(true);
      sess_->set_data(data);
    }
    /**
     * Get the session local data.
     * @return the session local data, or NULL if no data is registered.
     */
    Data* data() {
      _assert_(true);
      return (Data*)sess_->data();
    }
    /**
     * Get the expression of the socket.
     * @return the expression of the socket or an empty string on failure.
     */
    const std::string expression() {
      _assert_(true);
      return sess_->expression();
    }
  private:
    /**
     * Constructor.
     */
    explicit Session(HTTPServer::Session* sess) : sess_(sess) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    virtual ~Session() {
      _assert_(true);
    }
  private:
    HTTPServer::Session* sess_;
  };
  /**
   * Default constructor.
   */
  explicit RPCServer() : serv_(), worker_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~RPCServer() {
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
   * Set the worker to process each request.
   * @param worker the worker object.
   * @param thnum the number of worker threads.
   */
  void set_worker(Worker* worker, size_t thnum = 1) {
    _assert_(true);
    worker_.serv_ = this;
    worker_.worker_ = worker;
    serv_.set_worker(&worker_, thnum);
  }
  /**
   * Start the service.
   * @return true on success, or false on failure.
   * @note This function blocks until the server stops by the RPCServer::stop method.
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
  /**
   * Log a message.
   * @param kind the kind of the event.  Logger::DEBUG for debugging, Logger::INFO for normal
   * information, Logger::SYSTEM for system information, and Logger::ERROR for fatal error.
   * @param format the printf-like format string.  The conversion character `%' can be used with
   * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
   * @param ... used according to the format string.
   */
  void log(Logger::Kind kind, const char* format, ...) {
    _assert_(format);
    va_list ap;
    va_start(ap, format);
    serv_.log_v(kind, format, ap);
    va_end(ap);
  }
  /**
   * Log a message.
   * @note Equal to the original Cursor::set_value method except that the last parameters is
   * va_list.
   */
  void log_v(Logger::Kind kind, const char* format, va_list ap) {
    _assert_(format);
    serv_.log_v(kind, format, ap);
  }
private:
  /**
   * Adapter for the worker.
   */
  class WorkerAdapter : public HTTPServer::Worker {
    friend class RPCServer;
  public:
    WorkerAdapter() : serv_(NULL), worker_(NULL) {
      _assert_(true);
    }
  private:
    int32_t process(HTTPServer* serv, HTTPServer::Session* sess,
                    const std::string& path, HTTPClient::Method method,
                    const std::map<std::string, std::string>& reqheads,
                    const std::string& reqbody,
                    std::map<std::string, std::string>& resheads,
                    std::string& resbody,
                    const std::map<std::string, std::string>& misc) {
      const char* name = path.c_str();
      if (!kc::strfwm(name, RPCPATHPREFIX))
        return worker_->process(serv, sess, path, method, reqheads, reqbody,
                                resheads, resbody, misc);
      name += sizeof(RPCPATHPREFIX) - 1;
      std::map<std::string, std::string> inmap;
      const char* rp = strmapget(misc, "query");
      if (rp) wwwformtomap(rp, &inmap);
      rp = strmapget(reqheads, "content-type");
      if (rp) {
        if (kc::strifwm(rp, RPCFORMMTYPE)) {
          wwwformtomap(reqbody.c_str(), &inmap);
        } else if (kc::strifwm(rp, RPCTSVMTYPE)) {
          rp += sizeof(RPCTSVMTYPE) - 1;
          int32_t enc = 0;
          while (*rp != '\0') {
            while (*rp == ' ' || *rp == ';') {
              rp++;
            }
            if (kc::strifwm(rp, RPCTSVMATTR) && rp[sizeof(RPCTSVMATTR)-1] == '=') {
              rp += sizeof(RPCTSVMATTR);
              if (*rp == '"') rp++;
              switch (*rp) {
                case 'b': case 'B': enc = 'B'; break;
                case 'q': case 'Q': enc = 'Q'; break;
                case 'u': case 'U': enc = 'U'; break;
              }
            }
            while (*rp != '\0' && *rp != ' ' && *rp != ';') {
              rp++;
            }
          }
          tsvtomap(reqbody, &inmap);
          if (enc != 0) tsvmapdecode(&inmap, enc);
        }
      }
      std::map<std::string, std::string> outmap;
      Session mysess(sess);
      RPCClient::ReturnValue rv = worker_->process(serv_, &mysess, name, inmap, outmap);
      int32_t code = -1;
      switch (rv) {
        case RPCClient::RVSUCCESS: code = 200; break;
        case RPCClient::RVEINVALID: code = 400; break;
        case RPCClient::RVEINTERNAL: code = 500; break;
        case RPCClient::RVELOGIC: code = 450; break;
      }
      int32_t enc = checkmapenc(outmap);
      std::string outtype = RPCTSVMTYPE;
      switch (enc) {
        case 'B': outtype.append("; colenc=B"); break;
        case 'Q': outtype.append("; colenc=Q"); break;
        case 'U': outtype.append("; colenc=U"); break;
      }
      resheads["content-type"] = outtype;
      if (enc != 0) tsvmapencode(&outmap, enc);
      maptotsv(outmap, &resbody);
      return code;
    }
    RPCServer* serv_;
    RPCServer::Worker* worker_;
  };
  /** Dummy constructor to forbid the use. */
  RPCServer(const RPCServer&);
  /** Dummy Operator to forbid the use. */
  RPCServer& operator =(const RPCServer&);
  /** The internal server. */
  HTTPServer serv_;
  /** The adapter for worker. */
  WorkerAdapter worker_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
