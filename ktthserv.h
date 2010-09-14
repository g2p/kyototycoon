/*************************************************************************************************
 * Threaded Server
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


#ifndef _KTTHSERV_H                      // duplication check
#define _KTTHSERV_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktsocket.h>

namespace kyototycoon {                  // common namespace


/**
 * Interface of poolable I/O event.
 */
class ThreadedServer {
private:
  class TaskQueueImpl;
  class SocketTask;
public:
  /**
   * Interface to log internal information and errors.
   */
  class Logger {
  public:
    /**
     * Event kinds.
     */
    enum Kind {
      DEBUG = 1 << 0,                     ///< normal information
      INFO = 1 << 1,                     ///< normal information
      SYSTEM = 1 << 2,                   ///< system information
      ERROR = 1 << 3                     ///< error
    };
    /**
     * Destructor.
     */
    virtual ~Logger() {}
    /**
     * Process a log message.
     * @param kind the kind of the event.  Logger::DEBUG for debugging, Logger::INFO for normal
     * information, Logger::SYSTEM for system information, and Logger::ERROR for fatal error.
     * @param message the log message.
     */
    virtual void log(Kind kind, const char* message) = 0;
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
     * Process each request.
     * @param sock the socket to the client.
     * @param thid the ID number of the thread.  It can be from 0 to less than the number of
     * worker threads.
     * @return true to reuse the socket, or false to close the socket.
     */
    virtual bool process(Socket* sock, uint32_t thid) = 0;
  };
  /**
   * Default constructor.
   */
  ThreadedServer() :
    run_(false), expr_(), timeout_(0), worker_(NULL), thnum_(0), logger_(NULL), logkinds_(0),
    sock_(), poll_(), queue_(this) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~ThreadedServer() {
    _assert_(true);
  }
  /**
   * Set the network configurations.
   * @param expr an expression of the address and the port of the server.
   * @param timeout the timeout of each network operation in seconds.  If it is not more than 0,
   * no timeout is specified.
   */
  void set_network(const std::string& expr, double timeout = -1) {
    expr_ = expr;
    timeout_ = timeout;
  }
  /**
   * Set the worker to process each request.
   * @param worker the worker object.
   * @param thnum the number of worker threads.
   */
  void set_worker(Worker* worker, size_t thnum = 1) {
    _assert_(worker && thnum > 0 && thnum < kc::MEMMAXSIZ);
    worker_ = worker;
    thnum_ = thnum;
  }
  /**
   * Set the logger to process each log message.
   * @param logger the logger object.
   * @param kinds kinds of logged messages by bitwise-or: Logger::DEBUG for debugging,
   * Logger::INFO for normal information, Logger::SYSTEM for system information, and
   * Logger::ERROR for fatal error.
   */
  void set_logger(Logger* logger, uint32_t kinds = Logger::SYSTEM | Logger::ERROR) {
    _assert_(logger);
    logger_ = logger;
    logkinds_ = kinds;
  }
  /**
   * Start the service.
   * @return true on success, or false on failure.
   * @note This function blocks until the server stops by the ThreadedServer::stop method.
   */
  bool start() {
    log(Logger::SYSTEM, "================ [START]");
    log(Logger::SYSTEM, "starting the server");
    if (run_) {
      log(Logger::ERROR, "alreadiy running");
      return false;
    }
    if (expr_.empty()) {
      log(Logger::ERROR, "the network configuration is not set");
      return false;
    }
    if (!worker_) {
      log(Logger::ERROR, "the worker is not set");
      return false;
    }
    if (!sock_.open(expr_)) {
      log(Logger::ERROR, "socket error: expr=%s msg=%s", expr_.c_str(), sock_.error());
      return false;
    }
    log(Logger::SYSTEM, "server socket opened: expr=%s timeout=%.1f", expr_.c_str(), timeout_);
    if (!poll_.open()) {
      log(Logger::ERROR, "poller error: msg=%s", poll_.error());
      sock_.close();
      return false;
    }
    log(Logger::SYSTEM, "listening server socket started: fd=%d", sock_.descriptor());
    bool err = false;
    sock_.set_event_flags(Pollable::EVINPUT);
    if (!poll_.push(&sock_)) {
      log(Logger::ERROR, "poller error: msg=%s", poll_.error());
      err = true;
    }
    queue_.set_worker(worker_);
    queue_.start(thnum_);
    run_ = true;
    while (run_) {
      if (poll_.wait(0.1)) {
        Pollable* event;
        while ((event = poll_.pop()) != NULL) {
          if (event == &sock_) {
            Socket* csock = new Socket;
            csock->set_timeout(timeout_);
            if (sock_.accept(csock)) {
              log(Logger::INFO, "connected: expr=%s", csock->expression().c_str());
              csock->set_event_flags(Pollable::EVINPUT);
              if (!poll_.push(csock)) {
                log(Logger::ERROR, "poller error: msg=%s", poll_.error());
                err = true;
              }
            } else {
              log(Logger::ERROR, "socket error: msg=%s", sock_.error());
              err = true;
            }
            sock_.set_event_flags(Pollable::EVINPUT);
            if (!poll_.push(&sock_)) {
              log(Logger::ERROR, "poller error: msg=%s", poll_.error());
              err = true;
            }
          } else {
            Socket* csock = (Socket*)event;
            SocketTask* task = new SocketTask(csock);
            queue_.add_task(task);
          }
        }
      }
    }
    log(Logger::SYSTEM, "server stopped");
    if (err) log(Logger::SYSTEM, "one or more errors were detected");
    return !err;
  }
  /**
   * Stop the service.
   * @return true on success, or false on failure.
   */
  bool stop() {
    if (!run_) {
      log(Logger::ERROR, "not running");
      return false;
    }
    run_ = false;
    sock_.abort();
    poll_.abort();
    return true;
  }
  /**
   * Finish the service.
   * @return true on success, or false on failure.
   */
  bool finish() {
    log(Logger::SYSTEM, "finishing the server");
    if (run_) {
      log(Logger::ERROR, "not stopped");
      return false;
    }
    bool err = false;
    queue_.finish();
    if (queue_.error()) {
      log(Logger::SYSTEM, "one or more errors were detected");
      err = true;
    }
    if (poll_.flush()) {
      Pollable* event;
      while ((event = poll_.pop()) != NULL) {
        if (event == &sock_) continue;
        Socket* csock = (Socket*)event;
        log(Logger::INFO, "disconnecting: expr=%s", csock->expression().c_str());
        if (!csock->close()) {
          log(Logger::ERROR, "socket error: fd=%d msg=%s", csock->descriptor(), csock->error());
          err = true;
        }
        delete csock;
      }
    } else {
      log(Logger::ERROR, "poller error: msg=%s", poll_.error());
      err = true;
    }
    log(Logger::SYSTEM, "closing the server socket");
    if (!sock_.close()) {
      log(Logger::ERROR, "socket error: fd=%d msg=%s", sock_.descriptor(), sock_.error());
      err = true;
    }
    log(Logger::SYSTEM, "================ [FINISH]");
    return !err;
  }
private:
  /**
   * Task queue implementation.
   */
  class TaskQueueImpl : public kc::TaskQueue {
  public:
    TaskQueueImpl(ThreadedServer* serv) : serv_(serv), worker_(NULL), err_(false) {
      _assert_(true);
    }
    void set_worker(Worker* worker) {
      _assert_(worker);
      worker_ = worker;
    }
    bool error() {
      _assert_(true);
      return err_;
    }
  private:
    void do_task(kc::TaskQueue::Task* task) {
      _assert_(task);
      SocketTask* mytask = (SocketTask*)task;
      Socket* sock = mytask->sock_;
      bool keep = false;
      if (mytask->aborted()) {
        serv_->log(Logger::INFO, "aborted a request: expr=%s", sock->expression().c_str());
      } else {
        keep = worker_->process(sock, mytask->thread_id());
      }
      if (keep) {
        sock->set_event_flags(Pollable::EVINPUT);
        if (!serv_->poll_.push(sock)) {
          serv_->log(Logger::ERROR, "poller error: msg=%s", serv_->poll_.error());
          err_ = true;
        }
      } else {
        serv_->log(Logger::INFO, "disconnecting: expr=%s", sock->expression().c_str());
        if (!sock->close()) {
          serv_->log(Logger::ERROR, "socket error: msg=%s", sock->error());
          err_ = true;
        }
        delete sock;
      }
      delete mytask;
    }
    ThreadedServer* serv_;
    Worker* worker_;
    bool err_;
  };
  /**
   * Task with a socket.
   */
  class SocketTask : public kc::TaskQueue::Task {
    friend class ThreadedServer;
  public:
    SocketTask(Socket* sock) : sock_(sock) {}
  private:
    Socket* sock_;
  };
  /**
   * Log a message.
   */
  void log(Logger::Kind kind, const char* format, ...) {
    _assert_(format);
    if (!logger_ || !(kind & logkinds_)) return;
    std::string msg;
    va_list ap;
    va_start(ap, format);
    kc::vstrprintf(&msg, format, ap);
    va_end(ap);
    logger_->log(kind, msg.c_str());
  }
  /** Dummy constructor to forbid the use. */
  ThreadedServer(const ThreadedServer&);
  /** Dummy Operator to forbid the use. */
  ThreadedServer& operator =(const ThreadedServer&);
  /** The flag of running. */
  bool run_;
  /** The expression of the server socket. */
  std::string expr_;
  /** The timeout of each network operation. */
  double timeout_;
  /** The worker operator. */
  Worker* worker_;
  /** The number of worker threads. */
  size_t thnum_;
  /** The internal logger. */
  Logger* logger_;
  /** The kinds of logged messages. */
  uint32_t logkinds_;
  /** The server socket. */
  ServerSocket sock_;
  /** The event poller. */
  Poller poll_;
  /** The task queue. */
  TaskQueueImpl queue_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
