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
 * Escape meta characters in a string with the entity references of XML.
 * @param str the string.
 * @return the escaped string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* xmlescape(const char* str);


/**
 * Unescape meta characters in a string with the entity references of XML.
 * @param str the string.
 * @return the unescaped string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* xmlunescape(const char* str);


/**
 * Capitalize letters of a string.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strcapitalize(char* str);


/**
 * Get the Gregorian calendar of a time.
 * @param t the source time in seconds from the epoch.  If it is INT64_MAX, the current time is
 * specified.
 * @param jl the jet lag of a location in seconds.  If it is INT32_MAX, the local jet lag is
 * specified.
 * @param yearp the pointer to a variable to which the year is assigned.  If it is NULL, it is
 * not used.
 * @param monp the pointer to a variable to which the month is assigned.  If it is NULL, it is
 * not used.  1 means January and 12 means December.
 * @param dayp the pointer to a variable to which the day of the month is assigned.  If it is
 * NULL, it is not used.
 * @param hourp the pointer to a variable to which the hours is assigned.  If it is NULL, it is
 * not used.
 * @param minp the pointer to a variable to which the minutes is assigned.  If it is NULL, it is
 * not used.
 * @param secp the pointer to a variable to which the seconds is assigned.  If it is NULL, it is
 * not used.
 */
void getcalendar(int64_t t, int32_t jl,
                 int32_t* yearp = NULL, int32_t* monp = NULL, int32_t* dayp = NULL,
                 int32_t* hourp = NULL, int32_t* minp = NULL, int32_t* secp = NULL);


/**
 * Format a date as a string in W3CDTF.
 * @param t the source time in seconds from the epoch.  If it is INT64_MAX, the current time is
 * specified.
 * @param jl the jet lag of a location in seconds.  If it is INT32_MAX, the local jet lag is
 * specified.
 * @param buf the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 */
void datestrwww(int64_t t, int32_t jl, char* buf);


/**
 * Format a date as a string in RFC 1123 format.
 * @param t the source time in seconds from the epoch.  If it is INT64_MAX, the current time is
 * specified.
 * @param jl the jet lag of a location in seconds.  If it is INT32_MAX, the local jet lag is
 * specified.
 * @param buf the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 */
void datestrhttp(int64_t t, int32_t jl, char* buf);


/**
 * Get the time value of a date string.
 * @param str the date string in decimal, hexadecimal, W3CDTF, or RFC 822 (1123).  Decimal can
 * be trailed by "s" for in seconds, "m" for in minutes, "h" for in hours, and "d" for in days.
 * @return the time value of the date or INT64_MIN if the format is invalid.
 */
int64_t strmktime(const char* str);


/**
 * Get the jet lag of the local time.
 * @return the jet lag of the local time in seconds.
 */
int32_t jetlag();


/**
 * Get the day of week of a date.
 * @param year the year of a date.
 * @param mon the month of the date.
 * @param day the day of the date.
 * @return the day of week of the date.  0 means Sunday and 6 means Saturday.
 */
int32_t dayofweek(int32_t year, int32_t mon, int32_t day);


/**
 * Get the local time of a time.
 * @param time the time.
 * @param result the resulb buffer.
 * @return true on success, or false on failure.
 */
bool getlocaltime(time_t time, struct std::tm* result);


/**
 * Get the GMT local time of a time.
 * @param time the time.
 * @param result the resulb buffer.
 * @return true on success, or false on failure.
 */
bool getgmtime(time_t time, struct std::tm* result);


/**
 * Make the GMT from a time structure.
 * @param tm the pointer to the time structure.
 * @return the GMT.
 */
time_t mkgmtime(struct std::tm *tm);


/**
 * Set the signal handler for termination signals.
 */
inline bool setkillsignalhandler(void (*handler)(int)) {
  _assert_(handler);
  bool err = false;
  if (std::signal(SIGTERM, handler) == SIG_ERR) err = true;
  if (std::signal(SIGINT, handler) == SIG_ERR) err = true;
  if (std::signal(SIGHUP, handler) == SIG_ERR) err = true;
  return !err;
}


/**
 * Get the C-style string value of a record in a string map.
 */
inline const char* strmapget(const std::map<std::string, std::string>& map, const char* key) {
  _assert_(key);
  std::map<std::string, std::string>::const_iterator it = map.find(key);
  return it == map.end() ? NULL : it->second.c_str();
}


/**
 * Break up a URL into elements.
 */
inline void urlbreak(const char* url, std::map<std::string, std::string>* elems) {
  _assert_(url);
  char* trim = kc::strdup(url);
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
  (*elems)["self"] = rp;
  bool serv = false;
  if (kc::strifwm(rp, "http://")) {
    (*elems)["scheme"] = "http";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "https://")) {
    (*elems)["scheme"] = "https";
    rp += 8;
    serv = true;
  } else if (kc::strifwm(rp, "ftp://")) {
    (*elems)["scheme"] = "ftp";
    rp += 6;
    serv = true;
  } else if (kc::strifwm(rp, "sftp://")) {
    (*elems)["scheme"] = "sftp";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "ftps://")) {
    (*elems)["scheme"] = "ftps";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "tftp://")) {
    (*elems)["scheme"] = "tftp";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "ldap://")) {
    (*elems)["scheme"] = "ldap";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "ldaps://")) {
    (*elems)["scheme"] = "ldaps";
    rp += 8;
    serv = true;
  } else if (kc::strifwm(rp, "file://")) {
    (*elems)["scheme"] = "file";
    rp += 7;
    serv = true;
  }
  char* ep;
  if ((ep = std::strchr(rp, '#')) != NULL) {
    (*elems)["fragment"] = ep + 1;
    *ep = '\0';
  }
  if ((ep = std::strchr(rp, '?')) != NULL) {
    (*elems)["query"] = ep + 1;
    *ep = '\0';
  }
  if (serv) {
    if ((ep = std::strchr(rp, '/')) != NULL) {
      (*elems)["path"] = ep;
      *ep = '\0';
    } else {
      (*elems)["path"] = "/";
    }
    if ((ep = std::strchr(rp, '@')) != NULL) {
      *ep = '\0';
      if (rp[0] != '\0') (*elems)["authority"] = rp;
      rp = ep + 1;
    }
    if ((ep = std::strchr(rp, ':')) != NULL) {
      if (ep[1] != '\0') (*elems)["port"] = ep + 1;
      *ep = '\0';
    }
    if (rp[0] != '\0') (*elems)["host"] = rp;
  } else {
    (*elems)["path"] = rp;
  }
  delete[] norm;
  delete[] trim;
  const char* file = strmapget(*elems, "path");
  if (file) {
    const char* pv = std::strrchr(file, '/');
    if (pv) file = pv + 1;
    if (*file != '\0' && std::strcmp(file, ".") && std::strcmp(file, ".."))
      (*elems)["file"] = file;
  }
}


/**
 * Escape meta characters in a string with the entity references of XML.
 */
inline char* xmlescape(const char* str) {
  _assert_(str);
  const char* rp = str;
  size_t bsiz = 0;
  while (*rp != '\0') {
    switch (*rp) {
      case '&': bsiz += 5; break;
      case '<': bsiz += 4; break;
      case '>': bsiz += 4; break;
      case '"': bsiz += 6; break;
      default:  bsiz++; break;
    }
    rp++;
  }
  char* buf = new char[bsiz+1];
  char* wp = buf;
  while (*str != '\0') {
    switch (*str) {
      case '&': {
        std::memcpy(wp, "&amp;", 5);
        wp += 5;
        break;
      }
      case '<': {
        std::memcpy(wp, "&lt;", 4);
        wp += 4;
        break;
      }
      case '>': {
        std::memcpy(wp, "&gt;", 4);
        wp += 4;
        break;
      }
      case '"': {
        std::memcpy(wp, "&quot;", 6);
        wp += 6;
        break;
      }
      default: {
        *(wp++) = *str;
        break;
      }
    }
    str++;
  }
  *wp = '\0';
  return buf;
}


/**
 * Unescape meta characters in a string with the entity references of XML.
 */
inline char* xmlunescape(const char* str) {
  _assert_(str);
  char* buf = new char[std::strlen(str)+1];
  char* wp = buf;
  while (*str != '\0') {
    if (*str == '&') {
      if (kc::strfwm(str, "&amp;")) {
        *(wp++) = '&';
        str += 5;
      } else if (kc::strfwm(str, "&lt;")) {
        *(wp++) = '<';
        str += 4;
      } else if (kc::strfwm(str, "&gt;")) {
        *(wp++) = '>';
        str += 4;
      } else if (kc::strfwm(str, "&quot;")) {
        *(wp++) = '"';
        str += 6;
      } else {
        *(wp++) = *(str++);
      }
    } else {
      *(wp++) = *(str++);
    }
  }
  *wp = '\0';
  return buf;
}


/**
 * Capitalize letters of a string.
 */
inline char* strcapitalize(char* str) {
  _assert_(str);
  char* wp = str;
  bool head = true;
  while (*wp != '\0') {
    if (head && *wp >= 'a' && *wp <= 'z') *wp -= 'a' - 'A';
    head = *wp == '-' || *wp == ' ';
    wp++;
  }
  return str;
}


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
