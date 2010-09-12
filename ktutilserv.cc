/*************************************************************************************************
 * The server implementations to test miscellaneous utilities
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
kt::Server *g_server;                    // running server
kt::Poller *g_poller;                    // running poller


// function prototypes
int main(int argc, char** argv);
static void usage();
static void killserver(int signum);
static int32_t runecho(int argc, char** argv);
static int32_t procecho(const char* host, int32_t port, double timeout);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  kt::setkillsignalhandler(killserver);
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "echo")) {
    rv = runecho(argc, argv);
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
  eprintf("  %s echo [-host str] [-port num] [-tout num]\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// kill the running server
static void killserver(int signum) {
  iprintf("%s: catched the signal %d\n", g_progname, signum);
  if (g_server) {
    g_server->abort();
    g_server = NULL;
  }
  if (g_poller) {
    g_poller->abort();
    g_poller = NULL;
  }
}


// parse arguments of echo command
static int32_t runecho(int argc, char** argv) {
  const char* host = NULL;
  int32_t port = DEFPORT;
  double tout = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-host")) {
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
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procecho(host, port, tout);
  return rv;
}


// perform echo command
static int32_t procecho(const char* host, int32_t port, double timeout) {
  std::string addr = "";
  if (host) {
    addr = kt::Socket::get_host_address(host);
    if (addr.empty()) {
      eprintf("%s: %s: unknown host\n", g_progname, host);
      return 1;
    }
  }
  std::string expr = kc::strprintf("%s:%d", addr.c_str(), port);
  kt::Server serv;
  if (!serv.open(expr)) {
    eprintf("%s: server: open error: %s\n", g_progname, serv.error());
    return 1;
  }
  bool err = false;
  kt::Poller poll;
  if (!poll.open()) {
    eprintf("%s: poller: open error: %s\n", g_progname, poll.error());
    err = true;
  }
  g_server = &serv;
  g_poller = &poll;
  iprintf("%s: started: %s\n", g_progname, serv.expression().c_str());
  serv.set_event_flags(kt::Pollable::EVINPUT);
  if (!poll.push(&serv)) {
    eprintf("%s: poller: push error: %s\n", g_progname, poll.error());
    err = true;
  }
  while (g_server) {
    if (poll.wait()) {
      kt::Pollable* event;
      while ((event = poll.pop()) != NULL) {
        if (event == &serv) {
          kt::Socket* sock = new kt::Socket;
          sock->set_timeout(timeout);
          if (serv.accept(sock)) {
            iprintf("%s: connected: %s\n", g_progname, sock->expression().c_str());
            sock->set_event_flags(kt::Pollable::EVINPUT);
            if (!poll.push(sock)) {
              eprintf("%s: poller: push error: %s\n", g_progname, poll.error());
              err = true;
            }
          } else {
            eprintf("%s: server: accept error: %s\n", g_progname, serv.error());
            err = true;
            delete[] sock;
          }
          serv.set_event_flags(kt::Pollable::EVINPUT);
          if (!poll.push(&serv)) {
            eprintf("%s: poller: push error: %s\n", g_progname, poll.error());
            err = true;
          }
        } else {
          kt::Socket* sock = (kt::Socket*)event;
          char line[LINEBUFSIZ];
          if (sock->receive_line(line, sizeof(line))) {
            iprintf("%s: read: %s: %s\n", g_progname, sock->expression().c_str(), line);
            if (!std::strcmp(line, "/quit")) {
              iprintf("%s: closed: %s\n", g_progname, sock->expression().c_str());
              if (!sock->close()) {
                eprintf("%s: socket: close error: %s\n", g_progname, poll.error());
                err = true;
              }
              delete sock;
            } else {
              if (!sock->printf("%s\n", line)) {
                eprintf("%s: socket: printf error: %s\n", g_progname, sock->error());
                err = true;
              }
              serv.set_event_flags(kt::Pollable::EVINPUT);
              if (!poll.push(sock)) {
                eprintf("%s: poller: push error: %s\n", g_progname, poll.error());
                err = true;
              }
            }
          } else {
            iprintf("%s: closed: %s\n", g_progname, sock->expression().c_str());
            if (!sock->close()) {
              eprintf("%s: socket: close error: %s\n", g_progname, poll.error());
              err = true;
            }
            delete sock;
          }
        }
      }
    } else {
      eprintf("%s: poller: wait error: %s\n", g_progname, poll.error());
      err = true;
    }
  }
  g_server = NULL;
  g_poller = NULL;
  if (poll.flush()) {
    kt::Pollable* event;
    while ((event = poll.pop()) != NULL) {
      if (event != &serv) {
        kt::Socket* sock = (kt::Socket*)event;
        iprintf("%s: discarded: %s\n", g_progname, sock->expression().c_str());
        if (!sock->close()) {
          eprintf("%s: socket: close error: %s\n", g_progname, poll.error());
          err = true;
        }
        delete sock;
      }
    }
  } else {
    eprintf("%s: poller: flush error: %s\n", g_progname, poll.error());
    err = true;
  }
  iprintf("%s: finished: %s\n", g_progname, serv.expression().c_str());
  if (!poll.close()) {
    eprintf("%s: poller: close error: %s\n", g_progname, poll.error());
    err = true;
  }
  if (!serv.close()) {
    eprintf("%s: server: close error: %s\n", g_progname, serv.error());
    err = true;
  }
  return err ? 1 : 0;
}



// END OF FILE
