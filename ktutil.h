/*************************************************************************************************
 * Utility functions
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


#ifndef _KTUTIL_H                        // duplication check
#define _KTUTIL_H

#include <ktcommon.h>

namespace kyototycoon {                  // common namespace


/** The package version. */
extern const char* const VERSION;


/** The library version. */
extern const int32_t LIBVER;


/** The library revision. */
extern const int32_t LIBREV;


/**
 * Set the signal handler for termination signals.
 * @param handler the function pointer of the signal handler.
 * @return true on success, or false on failure.
 */
bool setkillsignalhandler(void (*handler)(int));


/**
 * Get the C-style string value of a record in a string map.
 * @param map the target string map.
 * @param key the key.
 * @return the C-style string value of the corresponding record, or NULL if there is no
 * corresponding record.
 */
const char* strmapget(const std::map<std::string, std::string>& map, const char* key);


/**
 * Break up a URL into elements.
 * @param url the URL string.
 * @param elems the map object to contain the result elements.  The key "self" indicates the URL
 * itself.  "scheme" indicates the scheme.  "host" indicates the host of the server.  "port"
 * indicates the port number of the server.  "authority" indicates the authority information.
 * "path" indicates the path of the resource.  "file" indicates the file name without the
 * directory section.  "query" indicates the query string.  "fragment" indicates the fragment
 * string.
 * @note Supported schema are HTTP, HTTPS, FTP, and FILE.  Both of absolute URL and relative URL
 * are supported.
 */
void urlbreak(const char* url, std::map<std::string, std::string>* elems);


/**
 * Capitalize letters of a string.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strcapitalize(char* str);


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
