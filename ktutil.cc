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


#include "ktutil.h"
#include "myconf.h"

namespace kyototycoon {                  // common namespace


/** The package version. */
const char* const VERSION = _KT_VERSION;


/** The library version. */
const int32_t LIBVER = _KT_LIBVER;


/** The library revision. */
const int32_t LIBREV = _KT_LIBREV;


/**
 * Set the signal handler for termination signals.
 */
bool setkillsignalhandler(void (*handler)(int)) {
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
const char* strmapget(const std::map<std::string, std::string>& map, const char* key) {
  _assert_(key);
  std::map<std::string, std::string>::const_iterator it = map.find(key);
  return it == map.end() ? NULL : it->second.c_str();
}


/**
 * Break up a URL into elements.
 */
void urlbreak(const char* url, std::map<std::string, std::string>* elems) {
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
 * Capitalize letters of a string.
 */
char* strcapitalize(char* str) {
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

// END OF FILE
