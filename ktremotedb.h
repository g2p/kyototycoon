/*************************************************************************************************
 * Remote database
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


#ifndef _KTREMOTEDB_H                    // duplication check
#define _KTREMOTEDB_H

#include <ktcommon.h>

namespace kyototycoon {                  // common namespace


/**
 * Constants for implementation.
 */
namespace {
}


/**
 * Remote database.
 * @note This class is a concrete class to access remote database servers.  This class can be
 * inherited but overwriting methods is forbidden.  Before every database operation, it is
 * necessary to call the RemoteDB::open method in order to connect to a database server.  To
 * avoid resource starvation it is important to close every database file by the RemoteDB::close
 * method when the connection is no longer in use.
 */
class RemoteDB {
public:
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
