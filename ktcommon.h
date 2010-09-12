/*************************************************************************************************
 * Common symbols for the library
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


#ifndef _KTCOMMON_H                      // duplication check
#define _KTCOMMON_H

#undef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS 1            ///< enable limit macros

#include <kccommon.h>
#include <kcutil.h>
#include <kcthread.h>
#include <kcdb.h>
#include <kcpolydb.h>

/**
 * All symbols of Kyoto Tycoon.
 */
namespace kyototycoon {
namespace kc = kyotocabinet;
}


#endif                                   // duplication check

// END OF FILE
