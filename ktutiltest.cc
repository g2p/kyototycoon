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


  std::map<std::string, std::string> map;

  /*
  kt::tsvtomap(argv[1], &map);
  kt::tsvmapdecode(&map, 'U');
  */
  kt::wwwformtomap(argv[1], &map);

  for (std::map<std::string, std::string>::iterator it = map.begin(); it != map.end(); it++) {

    printf("%s:%d  %s:%d\n",
           it->first.c_str(), (int)it->first.size(), it->second.c_str(), (int)it->second.size());

  }
  std::string form;
  kt::maptowwwform(map, &form);

  std::cout << form << std::endl;

  //printf("[%c]\n", kt::checkmapenc(map));






  //str.clear();
  //kt::maptotsv(map, &str);
  //std::cout << "[" << str << "]" << std::endl;





  return 0;
}



// END OF FILE
