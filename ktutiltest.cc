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

  const char *type = kt::HTTPServer::media_type(argv[1]);
  printf("type:%s\n", type ? type : "(null)");

  return 0;


  kt::URL url(argv[1]);
  kt::HTTPClient ua;
  printf("open: %d\n", ua.open(url.host(), url.port()));
  double stime = kc::time();
  for (int i = 0; i < 10000; i++) {
    int32_t code = ua.fetch(url.path_query());

    //printf("%d:%d\n", i, code);

    if (code != 200) {
      printf("error:%d\n", code);
      abort();
    }
  }
  double etime = kc::time();
  printf("%f\n", etime - stime);
  printf("close: %d\n", ua.close());



  /*
  std::string resbody;
  std::map<std::string, std::string> resheads;
  int32_t code = kt::HTTPClient::fetch_once(argv[1], kt::HTTPClient::MGET, &resbody, &resheads);
  printf("code:%d\n", code);


  std::map<std::string, std::string>::iterator it = resheads.begin();
  std::map<std::string, std::string>::iterator itend = resheads.end();
  while (it != itend) {
    std::cout << it->first << ": " << it->second << std::endl;
    it++;
  }
  std::cout << resbody << std::endl;
  */


  /*
  kt::URL url(argv[1]);


  kt::HTTPClient ua;
  printf("open: %d\n", ua.open(url.host(), url.port()));

  std::string resbody;
  std::map<std::string, std::string> resheads;
  int32_t code = ua.fetch(url.path_query(), kt::HTTPClient::MGET, &resbody, &resheads);
  printf("code:%d\n", code);


  std::map<std::string, std::string>::iterator it = resheads.begin();
  std::map<std::string, std::string>::iterator itend = resheads.end();
  while (it != itend) {
    std::cout << it->first << ": " << it->second << std::endl;
    it++;
  }
  std::cout << resbody << std::endl;



  code = ua.fetch("/", kt::HTTPClient::MGET, &resbody, &resheads);
  printf("code:%d\n", code);
  it = resheads.begin();
  itend = resheads.end();
  while (it != itend) {
    std::cout << it->first << ": " << it->second << std::endl;
    it++;
  }
  std::cout << resbody << std::endl;


  printf("close: %d\n", ua.close());

  */


  /*
  std::cout << url.expression() << std::endl;
  std::cout << url.path_query() << std::endl;

    std::cout << "scheme:[" << url.scheme() << "]" << std::endl;
    std::cout << "host:[" << url.host() << "]" << std::endl;
    std::cout << "port:[" << url.port() << "]" << std::endl;
    std::cout << "authority:[" << url.authority() << "]" << std::endl;
    std::cout << "path:[" << url.path() << "]" << std::endl;
    std::cout << "query:[" << url.query() << "]" << std::endl;
    std::cout << "fragment:[" << url.fragment() << "]" << std::endl;

  */




  return 0;
}



// END OF FILE
