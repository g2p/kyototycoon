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



int main(int argc, char** argv) {

  int64_t mem = memusage();

  kt::Poller poller;
  printf("open:%d\n", poller.open());

  const int32_t num = 16;


  for (int i = 0; i < num; i++) {
    kt::Socket* sock = new kt::Socket();
    if (sock->open("127.0.0.1:80")) {
      sock->set_event_flags(kt::Pollable::EVOUTPUT);
      poller.push(sock);
    } else {
      delete sock;
      printf("fail\n");
    }
  }

  printf("count:%d\n", (int)poller.count());

  // usage:4194304
  // usage:139264

  printf("wait:%d\n", poller.wait());
  std::cout << poller.error() << std::endl;

  printf("count:%d\n", (int)poller.count());

  kt::Pollable* event;
  while ((event = poller.pop()) != NULL) {
    kt::Socket* s = (kt::Socket*)event;
    printf("%u\n", s->event_flags());
    std::cout << s->expression() << std::endl;
    s->printf("GET / HTTP/1.1\r\n");
    s->printf("Host: localhost\r\n");
    s->printf("\r\n");
    s->set_event_flags(kt::Pollable::EVINPUT | kt::Pollable::EVOUTPUT);
    printf("push:%d\n", poller.push(s));
  }
  printf("---\n");

  printf("count:%d\n", (int)poller.count());
  printf("flush:%d\n", poller.flush());
  std::cout << poller.error() << std::endl;

  int cnt = 0;
  while ((event = poller.pop()) != NULL) {
    cnt++;
    kt::Socket* s = (kt::Socket*)event;

    printf("%u\n", s->event_flags());

    std::cout << cnt << ":" << s->expression() << std::endl;
    delete s;
  }



  printf("close:%d\n", poller.close());

  printf("usage:%lld\n", (long long)(memusage() - mem));







  /*
  kt::Server serv;
  serv.set_timeout(10.0);
  printf("open:%d\n", serv.open(argv[1]));

  printf("open:%s\n", serv.error());

  std::cout << serv.expression() << std::endl;


    kt::Socket sock;
    printf("accept:%d\n", serv.accept(&sock));
    std::cout << sock.expression() << std::endl;

    sock.printf("HTTP/1.1 200 OK\r\n");
    sock.printf("Content-Type: text/plain\r\n");
    sock.printf("Connection: close\r\n");
    sock.printf("\r\n");
    sock.printf("fuck\n");


    sock.close();





  printf("close:%d\n", serv.close());

  printf("close:%s\n", serv.error());
  */





    /*
  std::cout << kt::Socket::get_local_host_name() << std::endl;
  std::cout << kt::Socket::get_host_address("localhost") << std::endl;
  std::cout << kt::Socket::get_host_address("kyoto") << std::endl;
    */


  return 0;
}



// END OF FILE
