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


kt::ThreadedServer* g_server = NULL;



// kill the running server
void killserver(int signum) {
  iprintf("catched the signal %d\n", signum);
  if (g_server) {
    g_server->stop();
    g_server = NULL;
  }
}

int main(int argc, char** argv) {
  kc::setstdiobin();
  kt::setkillsignalhandler(killserver);
  kt::ThreadedServer serv;
  class MyServer : public kt::ThreadedServer::Worker {
  private:
    bool process(kt::Socket* sock, uint32_t thid) {
      std::printf("process:%d\n", thid);
      char line[1024];
      if (sock->receive_line(line, sizeof(line))) {
        std::printf("%s\n", line);
        sock->printf(":::%s\n", line);
      }
      return true;
    }
  } worker;
  serv.set_network(":1978", 10);
  serv.set_worker(&worker, 2);
  serv.set_logger(stdlogger(argv[0], &std::cout), UINT32_MAX);
  g_server = &serv;
  serv.start();
  serv.finish();
  return 0;
}



// END OF FILE
