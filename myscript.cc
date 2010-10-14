/*************************************************************************************************
 * The scripting extension
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


#include "myscript.h"
#include "myconf.h"


#if _KT_LUA


extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}


/* precedent type declaration */
struct ScriptProcessorCore;
class StringBuilder;
struct SoftCursor;
struct SoftDB;
class SoftVisitor;
class SoftFileProcessor;
typedef std::map<std::string, std::string> StringMap;


/* function prototypes */
static void reporterror(ScriptProcessorCore* core, const char* name);
static void setfielduint(lua_State* lua, const char* name, uint32_t num);
static void setfieldstr(lua_State* lua, const char* name, const char* str);
static void setfieldfunc(lua_State* lua, const char* name, lua_CFunction func);
static void setfieldvalue(lua_State* lua, const char* name, int32_t index);
static void define_module(lua_State* lua);
static int kt_atoi(lua_State* lua);
static int kt_atoix(lua_State* lua);
static int kt_atof(lua_State* lua);
static int kt_hash_murmur(lua_State* lua);
static int kt_hash_fnv(lua_State* lua);
static int kt_time(lua_State* lua);
static int kt_sleep(lua_State* lua);
static int kt_pack(lua_State* lua);
static int kt_unpack(lua_State *lua);
static int kt_split(lua_State *lua);
static int kt_codec(lua_State *lua);
static int kt_bit(lua_State *lua);
static int kt_strstr(lua_State *lua);
static int kt_strfwm(lua_State *lua);
static int kt_strbwm(lua_State *lua);
static void define_err(lua_State* lua);
static int err_new(lua_State* lua);
static int err_tostring(lua_State* lua);
static int err_set(lua_State* lua);
static int err_code(lua_State* lua);
static int err_name(lua_State* lua);
static int err_message(lua_State* lua);
static void define_vis(lua_State* lua);
static int vis_new(lua_State* lua);
static int vis_visit_full(lua_State* lua);
static int vis_visit_empty(lua_State* lua);
static void define_fproc(lua_State* lua);
static int fproc_new(lua_State* lua);
static int fproc_process(lua_State* lua);
static void define_cur(lua_State* lua);
static int cur_new(lua_State* lua);
static int cur_gc(lua_State* lua);
static int cur_tostring(lua_State* lua);
static int cur_call(lua_State* lua);
static int cur_disable(lua_State* lua);
static int cur_accept(lua_State* lua);
static int cur_set_value(lua_State* lua);
static int cur_remove(lua_State* lua);
static int cur_get_key(lua_State* lua);
static int cur_get_value(lua_State* lua);
static int cur_get(lua_State* lua);
static int cur_jump(lua_State* lua);
static int cur_jump_back(lua_State* lua);
static int cur_step(lua_State* lua);
static int cur_step_back(lua_State* lua);
static int cur_db(lua_State* lua);
static int cur_error(lua_State* lua);
static void define_db(lua_State* lua);
static int db_new(lua_State* lua);
static int db_gc(lua_State* lua);
static int db_tostring(lua_State* lua);
static int db_index(lua_State* lua);
static int db_newindex(lua_State* lua);
static int db_new_ptr(lua_State* lua);
static int db_delete_ptr(lua_State* lua);
static int db_process(lua_State* lua);
static int db_error(lua_State* lua);
static int db_open(lua_State* lua);
static int db_close(lua_State* lua);
static int db_accept(lua_State* lua);
static int db_iterate(lua_State* lua);
static int db_set(lua_State* lua);
static int db_add(lua_State* lua);
static int db_replace(lua_State* lua);
static int db_append(lua_State* lua);
static int db_increment(lua_State* lua);
static int db_increment_double(lua_State* lua);
static int db_cas(lua_State* lua);
static int db_remove(lua_State* lua);
static int db_get(lua_State* lua);
static int db_clear(lua_State* lua);
static int db_synchronize(lua_State* lua);
static int db_copy(lua_State* lua);
static int db_begin_transaction(lua_State* lua);
static int db_end_transaction(lua_State* lua);
static int db_transaction(lua_State* lua);
static int db_dump_snapshot(lua_State* lua);
static int db_load_snapshot(lua_State* lua);
static int db_count(lua_State* lua);
static int db_size(lua_State* lua);
static int db_path(lua_State* lua);
static int db_status(lua_State* lua);
static int db_cursor(lua_State* lua);
static int db_cursor_process(lua_State* lua);
static int db_pairs(lua_State* lua);
static int serv_log(lua_State* lua);


/**
 * ScriptProcessor internal.
 */
struct ScriptProcessorCore {
  std::string path;
  int32_t thid;
  kt::RPCServer* serv;
  kt::TimedDB* dbs;
  int32_t dbnum;
  const std::map<std::string, int32_t>* dbmap;
  lua_State *lua;
};


/**
 * Wrapper of a string.
 */
class StringBuilder {
public:
  StringBuilder(lua_State* lua, int32_t index) : str_(), ok_(false) {
    char nbuf[kc::NUMBUFSIZ];
    const char* ptr = NULL;
    size_t size = 0;
    switch (lua_type(lua, index)) {
      case LUA_TNUMBER: {
        double num = lua_tonumber(lua, index);
        if (num == std::floor(num)) {
          std::snprintf(nbuf, sizeof(nbuf) - 1, "%lld", (long long)num);
        } else {
          std::snprintf(nbuf, sizeof(nbuf) - 1, "%f", num);
        }
        nbuf[sizeof(nbuf)-1] = '\0';
        ptr = nbuf;
        size = std::strlen(ptr);
        break;
      }
      case LUA_TBOOLEAN: {
        ptr = lua_toboolean(lua, index) ? "true" : "false";
        size = std::strlen(ptr);
        break;
      }
      case LUA_TSTRING: {
        ptr = lua_tolstring(lua, index, &size);
        break;
      }
    }
    if (ptr) {
      str_.append(ptr, size);
      ok_ = true;
    }
  }
  const std::string& str() {
    return str_;
  }
  bool ok() {
    return ok_;
  }
private:
  std::string str_;
  bool ok_;
};


/**
 * Default constructor.
 */
ScriptProcessor::ScriptProcessor() {
  _assert_(true);
  ScriptProcessorCore* core = new ScriptProcessorCore;
  core->thid = 0;
  core->serv = NULL;
  core->dbs = NULL;
  core->dbnum = 0;
  core->dbmap = NULL;
  core->lua = NULL;
  opq_ = core;
}


/**
 * Destructor.
 */
ScriptProcessor::~ScriptProcessor() {
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  lua_State* lua = core->lua;
  if (lua) lua_close(lua);
  delete core;
}


/**
 * Set domain-specific resources.
 */
bool ScriptProcessor::set_resources(int32_t thid, kt::RPCServer* serv,
                                    kt::TimedDB* dbs, int32_t dbnum,
                                    const std::map<std::string, int32_t>* dbmap) {
  _assert_(serv && dbs && dbnum >= 0 && dbmap);
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  lua_State *lua = luaL_newstate();
  if (!lua) return false;
  luaL_openlibs(lua);
  lua_settop(lua, 0);
  lua_newtable(lua);
  define_module(lua);
  define_err(lua);
  define_vis(lua);
  define_fproc(lua);
  define_cur(lua);
  define_db(lua);
  lua_setglobal(lua, "__kyototycoon__");
  lua_getglobal(lua, "__kyototycoon__");
  lua_pushlightuserdata(lua, serv);
  lua_setfield(lua, -2, "__serv__");
  setfieldstr(lua, "VERSION", kt::VERSION);
  setfielduint(lua, "RVSUCCESS", kt::RPCClient::RVSUCCESS);
  setfielduint(lua, "RVENOIMPL", kt::RPCClient::RVENOIMPL);
  setfielduint(lua, "RVEINVALID", kt::RPCClient::RVEINVALID);
  setfielduint(lua, "RVELOGIC", kt::RPCClient::RVELOGIC);
  setfielduint(lua, "RVEINTERNAL", kt::RPCClient::RVEINTERNAL);
  setfielduint(lua, "thid", thid);
  lua_getfield(lua, -1, "DB");
  lua_newtable(lua);
  for (int32_t i = 0; i < dbnum; i++) {
    lua_getfield(lua, -2, "new");
    lua_pushvalue(lua, -3);
    lua_pushlightuserdata(lua, dbs + i);
    if (lua_pcall(lua, 2, 1, 0) != 0) {
      lua_close(lua);
      return false;
    }
    lua_rawseti(lua, -2, i + 1);
  }
  lua_rawgeti(lua, -1, 1);
  lua_setfield(lua, -4, "db");
  std::map<std::string, int32_t>::const_iterator it = dbmap->begin();
  std::map<std::string, int32_t>::const_iterator itend = dbmap->end();
  while (it != itend) {
    lua_rawgeti(lua, -1, it->second);
    lua_setfield(lua, -2, it->first.c_str());
    it++;
  }
  lua_setfield(lua, -3, "dbs");
  lua_pop(lua, 1);
  setfieldfunc(lua, "log", serv_log);
  lua_settop(lua, 0);
  core->thid = thid;
  core->serv = serv;
  core->dbs = dbs;
  core->dbnum = dbnum;
  core->dbmap = dbmap;
  core->lua = lua;
  return true;
}


/**
 * Load a script file.
 */
bool ScriptProcessor::load(const std::string& path) {
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  core->path = path;
  lua_State* lua = core->lua;
  int64_t size;
  char* script = kc::File::read_file(path, &size);
  if (!script) return false;
  bool err = false;
  lua_settop(lua, 0);
  if (luaL_loadstring(lua, script) != 0 || lua_pcall(lua, 0, 0, 0) != 0) {
    reporterror(core, "(init)");
    err = true;
  }
  delete[] script;
  return !err;
}


/**
 * Call a procedure.
 */
kt::RPCClient::ReturnValue ScriptProcessor::call(const std::string& name,
                                                 const std::map<std::string, std::string>& inmap,
                                                 std::map<std::string, std::string>& outmap) {
  _assert_(true);
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  lua_State* lua = core->lua;
  lua_settop(lua, 0);
  lua_newtable(lua);
  lua_getglobal(lua, name.c_str());
  if (!lua_isfunction(lua, -1)) {
    lua_settop(lua, 0);
    return kt::RPCClient::RVEINVALID;
  }
  lua_newtable(lua);
  std::map<std::string, std::string>::const_iterator it = inmap.begin();
  std::map<std::string, std::string>::const_iterator itend = inmap.end();
  while (it != itend) {
    lua_pushlstring(lua, it->second.data(), it->second.size());
    lua_setfield(lua, -2, it->first.c_str());
    it++;
  }
  lua_pushvalue(lua, 1);
  kt::RPCClient::ReturnValue rv;
  if (lua_pcall(lua, 2, 1, 0) == 0) {
    lua_pushnil(lua);
    while (lua_next(lua, 1) != 0) {
      StringBuilder key(lua, -2);
      StringBuilder value(lua, -1);
      if (key.ok() && value.ok()) outmap[key.str()] = value.str();
      lua_pop(lua, 1);
    }
    rv = (kt::RPCClient::ReturnValue)lua_tointeger(lua, -1);
  } else {
    reporterror(core, name.c_str());
    rv = kt::RPCClient::RVEINTERNAL;
  }
  lua_settop(lua, 0);
  return rv;
}


/**
 * Burrow of cursors no longer in use.
 */
class CursorBurrow {
private:
  typedef std::vector<kt::TimedDB::Cursor*> CursorList;
public:
  explicit CursorBurrow() : lock_(), dcurs_() {}
  ~CursorBurrow() {
    sweap();
  }
  void sweap() {
    kc::ScopedSpinLock lock(&lock_);
    if (dcurs_.size() > 0) {
      CursorList::iterator dit = dcurs_.begin();
      CursorList::iterator ditend = dcurs_.end();
      while (dit != ditend) {
        kt::TimedDB::Cursor* cur = *dit;
        delete cur;
        dit++;
      }
      dcurs_.clear();
    }
  }
  void deposit(kt::TimedDB::Cursor* cur) {
    kc::ScopedSpinLock lock(&lock_);
    dcurs_.push_back(cur);
  }
private:
  kc::SpinLock lock_;
  CursorList dcurs_;
} g_curbur;


/**
 * Wrapper of a database.
 */
struct SoftCursor {
  kt::TimedDB::Cursor* cur;
};


/**
 * Wrapper of a database.
 */
struct SoftDB {
  kt::TimedDB* db;
  bool light;
};


/**
 * Wrapper of a visitor.
 */
class SoftVisitor : public kt::TimedDB::Visitor {
public:
  explicit SoftVisitor(lua_State* lua, bool writable) :
    lua_(lua), writable_(writable), top_(0) {
    top_ = lua_gettop(lua_);
  }
private:
  const char* visit_full(const char* kbuf, size_t ksiz,
                         const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
    lua_settop(lua_, top_);
    int32_t argc;
    if (lua_istable(lua_, -1)) {
      lua_getfield(lua_, -1, "visit_full");
      lua_pushvalue(lua_, -2);
      lua_pushlstring(lua_, kbuf, ksiz);
      lua_pushlstring(lua_, vbuf, vsiz);
      lua_pushnumber(lua_, *xtp < kt::TimedDB::XTMAX ? *xtp : kt::TimedDB::XTMAX);
      argc = 4;
    } else {
      lua_pushvalue(lua_, -1);
      lua_pushlstring(lua_, kbuf, ksiz);
      lua_pushlstring(lua_, vbuf, vsiz);
      lua_pushnumber(lua_, *xtp < kt::TimedDB::XTMAX ? *xtp : kt::TimedDB::XTMAX);
      argc = 3;
    }
    const char* rv = NOP;
    if (lua_pcall(lua_, argc, 2, 0) == 0) {
      if (lua_islightuserdata(lua_, -1)) {
        const char* trv = (const char*)lua_touserdata(lua_, -1);
        if (trv == kt::TimedDB::Visitor::REMOVE) rv = kt::TimedDB::Visitor::REMOVE;
      } else {
        size_t rsiz;
        const char* rbuf = lua_tolstring(lua_, -2, &rsiz);
        if (rbuf) {
          rv = rbuf;
          *sp = rsiz;
        }
        *xtp = lua_isnumber(lua_, -1) ? lua_tonumber(lua_, -1) : -*xtp;
      }
    }
    if (!writable_) rv = NULL;
    return rv;
  }
  const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
    lua_settop(lua_, top_);
    int32_t argc;
    if (lua_istable(lua_, -1)) {
      lua_getfield(lua_, -1, "visit_empty");
      lua_pushvalue(lua_, -2);
      lua_pushlstring(lua_, kbuf, ksiz);
      argc = 2;
    } else {
      lua_pushvalue(lua_, -1);
      lua_pushlstring(lua_, kbuf, ksiz);
      lua_pushnil(lua_);
      argc = 2;
    }
    const char* rv = NOP;
    if (lua_pcall(lua_, argc, 2, 0) == 0) {
      if (lua_islightuserdata(lua_, -1)) {
        const char* trv = (const char*)lua_touserdata(lua_, -1);
        if (trv == kt::TimedDB::Visitor::REMOVE) rv = kt::TimedDB::Visitor::REMOVE;
      } else {
        size_t rsiz;
        const char* rbuf = lua_tolstring(lua_, -2, &rsiz);
        if (rbuf) {
          rv = rbuf;
          *sp = rsiz;
        }
        *xtp = lua_isnumber(lua_, -1) ? lua_tonumber(lua_, -1) : INT64_MAX;
      }
    }
    if (!writable_) rv = NULL;
    return rv;
  }
  lua_State* lua_;
  bool writable_;
  int32_t top_;
};


/**
 * Wrapper of a file processor.
 */
class SoftFileProcessor : public kc::BasicDB::FileProcessor {
public:
  explicit SoftFileProcessor(lua_State* lua) : lua_(lua) {}
private:
  bool process(const std::string& path, int64_t count, int64_t size) {
    int32_t argc;
    if (lua_istable(lua_, -1)) {
      lua_getfield(lua_, -1, "process");
      lua_pushvalue(lua_, -2);
      lua_pushstring(lua_, path.c_str());
      lua_pushnumber(lua_, count);
      lua_pushnumber(lua_, size);
      argc = 4;
    } else {
      lua_pushvalue(lua_, -1);
      lua_pushstring(lua_, path.c_str());
      lua_pushnumber(lua_, count);
      lua_pushnumber(lua_, size);
      argc = 3;
    }
    bool rv = false;
    if (lua_pcall(lua_, argc, 1, 0) == 0) rv = lua_toboolean(lua_, -1);
    return rv;
  }
  lua_State* lua_;
};


/**
 * Report error information.
 */
static void reporterror(ScriptProcessorCore* core, const char* name) {
  lua_State* lua = core->lua;
  int argc = lua_gettop(lua);
  core->serv->log(kt::RPCServer::Logger::ERROR, "[SCRIPT]: %s: %s",
                  name, argc > 0 ? lua_tostring(lua, argc) : "unknown");
}


/**
 * throw the invalid argument error.
 */
static void throwinvarg(lua_State* lua) {
  lua_pushstring(lua, "invalid arguments");
  lua_error(lua);
}


/**
 * Set a field of unsigned integer.
 */
static void setfielduint(lua_State* lua, const char* name, uint32_t num) {
  lua_pushinteger(lua, num);
  lua_setfield(lua, -2, name);
}


/**
 * Set a field of string.
 */
static void setfieldstr(lua_State* lua, const char* name, const char* str) {
  lua_pushstring(lua, str);
  lua_setfield(lua, -2, name);
}


/**
 * Set a field of function.
 */
static void setfieldfunc(lua_State* lua, const char* name, lua_CFunction func) {
  lua_pushcfunction(lua, func);
  lua_setfield(lua, -2, name);
}


/**
 * Set a field of stacked value.
 */
static void setfieldvalue(lua_State* lua, const char* name, int32_t index) {
  lua_pushvalue(lua, index);
  lua_setfield(lua, -2, name);
}


/**
 * Define objects of the module.
 */
static void define_module(lua_State* lua) {
  setfieldfunc(lua, "atoi", kt_atoi);
  setfieldfunc(lua, "atoix", kt_atoix);
  setfieldfunc(lua, "atof", kt_atof);
  setfieldfunc(lua, "hash_murmur", kt_hash_murmur);
  setfieldfunc(lua, "hash_fnv", kt_hash_fnv);
  setfieldfunc(lua, "time", kt_time);
  setfieldfunc(lua, "sleep", kt_sleep);
  setfieldfunc(lua, "pack", kt_pack);
  setfieldfunc(lua, "unpack", kt_unpack);
  setfieldfunc(lua, "split", kt_split);
  setfieldfunc(lua, "codec", kt_codec);
  setfieldfunc(lua, "bit", kt_bit);
  setfieldfunc(lua, "strstr", kt_strstr);
  setfieldfunc(lua, "strfwm", kt_strfwm);
  setfieldfunc(lua, "strbwm", kt_strbwm);
}


/**
 * Implementation of atoi.
 */
static int kt_atoi(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1) throwinvarg(lua);
  const char* str = lua_tostring(lua, 1);
  if (!str) return 0;
  lua_pushnumber(lua, kc::atoi(str));
  return 1;
}


/**
 * Implementation of atoix.
 */
static int kt_atoix(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1) throwinvarg(lua);
  const char* str = lua_tostring(lua, 1);
  if (!str) return 0;
  lua_pushnumber(lua, kc::atoix(str));
  return 1;
}


/**
 * Implementation of atof.
 */
static int kt_atof(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1) throwinvarg(lua);
  const char* str = lua_tostring(lua, 1);
  if (!str) return 0;
  lua_pushnumber(lua, kc::atof(str));
  return 1;
}


/**
 * Implementation of hash_murmur.
 */
static int kt_hash_murmur(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1) throwinvarg(lua);
  size_t len;
  const char* str = lua_tolstring(lua, 1, &len);
  if (!str) return 0;
  lua_pushinteger(lua, kc::hashmurmur(str, len) & INT32_MAX);
  return 1;
}


/**
 * Implementation of hash_fnv.
 */
static int kt_hash_fnv(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1) throwinvarg(lua);
  size_t len;
  const char* str = lua_tolstring(lua, 1, &len);
  if (!str) return 0;
  lua_pushinteger(lua, kc::hashfnv(str, len) & INT32_MAX);
  return 1;
}


/**
 * Implementation of hash_fnv.
 */
static int kt_time(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 0) throwinvarg(lua);
  lua_pushnumber(lua, kc::time());
  return 1;
}


/**
 * Implementation of sleep.
 */
static int kt_sleep(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc > 1) throwinvarg(lua);
  double sec = argc > 0 ? lua_tonumber(lua, 1) : 0;
  if (sec > 0) {
    kc::Thread::sleep(sec);
  } else {
    kc::Thread::yield();
  }
  return 0;
}


/**
 * Implementation of pack.
 */
static int kt_pack(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1) throwinvarg(lua);
  const char* format = lua_tostring(lua, 1);
  if (!format) throwinvarg(lua);
  lua_newtable(lua);
  int32_t aidx = argc + 1;
  int32_t eidx = 1;
  for (int32_t i = 2; i <= argc; i++) {
    size_t len;
    switch (lua_type(lua, i)) {
      case LUA_TNUMBER: case LUA_TSTRING: {
        lua_pushvalue(lua, i);
        lua_rawseti(lua, aidx, eidx++);
        break;
      }
      case LUA_TTABLE: {
        len = lua_objlen(lua, i);
        for (size_t j = 1; j <= len; j++) {
          lua_rawgeti(lua, i, j);
          lua_rawseti(lua, aidx, eidx++);
        }
        break;
      }
      default: {
        lua_pushnumber(lua, 0);
        lua_rawseti(lua, aidx, eidx++);
        break;
      }
    }
  }
  lua_replace(lua, 2);
  lua_settop(lua, 2);
  std::string xstr;
  int32_t emax = eidx - 1;
  eidx = 1;
  while (*format != '\0') {
    int32_t c = *format;
    int32_t loop = 1;
    if (format[1] == '*') {
      loop = INT32_MAX;
      format++;
    } else if (format[1] >= '0' && format[1] <= '9') {
      char* suffix;
      loop = std::strtol(format + 1, &suffix, 10);
      format = suffix - 1;
    }
    loop = loop < emax ? loop : emax;
    int32_t end = eidx + loop - 1;
    if (end > emax) end = emax;
    while (eidx <= end) {
      lua_rawgeti(lua, 2, eidx);
      double num = lua_tonumber(lua, 3);
      lua_pop(lua, 1);
      uint8_t cnum;
      uint16_t snum;
      uint32_t inum;
      uint64_t lnum;
      double dnum;
      float fnum;
      uint64_t wnum;
      char wbuf[kc::NUMBUFSIZ], *wp;
      switch (c) {
        case 'c': case 'C': {
          cnum = num;
          xstr.append((char*)&cnum, sizeof(cnum));
          break;
        }
        case 's': case 'S': {
          snum = num;
          xstr.append((char*)&snum, sizeof(snum));
          break;
        }
        case 'i': case 'I': {
          inum = num;
          xstr.append((char*)&inum, sizeof(inum));
          break;
        }
        case 'l': case 'L': {
          lnum = num;
          xstr.append((char*)&lnum, sizeof(lnum));
          break;
        }
        case 'f': case 'F': {
          fnum = num;
          xstr.append((char*)&fnum, sizeof(fnum));
          break;
        }
        case 'd': case 'D': {
          dnum = num;
          xstr.append((char*)&dnum, sizeof(dnum));
          break;
        }
        case 'n': {
          snum = num;
          snum = kc::hton16(snum);
          xstr.append((char*)&snum, sizeof(snum));
          break;
        }
        case 'N': {
          inum = num;
          inum = kc::hton32(inum);
          xstr.append((char*)&inum, sizeof(inum));
          break;
        }
        case 'M': {
          lnum = num;
          lnum = kc::hton64(lnum);
          xstr.append((char*)&lnum, sizeof(lnum));
          break;
        }
        case 'w': case 'W': {
          wnum = num;
          wp = wbuf;
          if (wnum < (1ULL << 7)) {
            *(wp++) = wnum;
          } else if (wnum < (1ULL << 14)) {
            *(wp++) = (wnum >> 7) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if (wnum < (1ULL << 21)) {
            *(wp++) = (wnum >> 14) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if (wnum < (1ULL << 28)) {
            *(wp++) = (wnum >> 21) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if (wnum < (1ULL << 35)) {
            *(wp++) = (wnum >> 28) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if (wnum < (1ULL << 42)) {
            *(wp++) = (wnum >> 35) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if (wnum < (1ULL << 49)) {
            *(wp++) = (wnum >> 42) | 0x80;
            *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if (wnum < (1ULL << 56)) {
            *(wp++) = (wnum >> 49) | 0x80;
            *(wp++) = ((wnum >> 42) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else {
            *(wp++) = (wnum >> 63) | 0x80;
            *(wp++) = ((wnum >> 49) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 42) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          }
          xstr.append(wbuf, wp - wbuf);
          break;
        }
      }
      eidx++;
    }
    format++;
    if (eidx > emax) break;
  }
  lua_pushlstring(lua, xstr.data(), xstr.size());
  return 1;
}


/**
 * Implementation of unpack.
 */
static int kt_unpack(lua_State *lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2) throwinvarg(lua);
  const char* format = lua_tostring(lua, 1);
  size_t size;
  const char* buf = lua_tolstring(lua, 2, &size);
  if (!format) throwinvarg(lua);
  if (!buf) {
    buf = "";
    size = 0;
  }
  lua_newtable(lua);
  const char* rp = buf;
  int32_t eidx = 1;
  while (*format != '\0') {
    int32_t c = *format;
    int32_t loop = 1;
    if (format[1] == '*') {
      loop = INT32_MAX;
      format++;
    } else if (format[1] >= '0' && format[1] <= '9') {
      char* suffix;
      loop = strtol(format + 1, &suffix, 10);
      format = suffix - 1;
    }
    loop = loop < (int32_t)size ? loop : size;
    for (int32_t i = 0; i < loop && size > 0; i++) {
      uint8_t cnum;
      uint16_t snum;
      uint32_t inum;
      uint64_t lnum;
      float fnum;
      double dnum;
      uint64_t wnum;
      switch (c) {
        case 'c': {
          if (size >= sizeof(cnum)) {
            std::memcpy(&cnum, rp, sizeof(cnum));
            lua_pushnumber(lua, (int8_t)cnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(cnum);
            size -= sizeof(cnum);
          } else {
            size = 0;
          }
          break;
        }
        case 'C': {
          if (size >= sizeof(cnum)) {
            std::memcpy(&cnum, rp, sizeof(cnum));
            lua_pushnumber(lua, (uint8_t)cnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(cnum);
            size -= sizeof(cnum);
          } else {
            size = 0;
          }
          break;
        }
        case 's': {
          if (size >= sizeof(snum)) {
            std::memcpy(&snum, rp, sizeof(snum));
            lua_pushnumber(lua, (int16_t)snum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(snum);
            size -= sizeof(snum);
          } else {
            size = 0;
          }
          break;
        }
        case 'S': {
          if (size >= sizeof(snum)) {
            std::memcpy(&snum, rp, sizeof(snum));
            lua_pushnumber(lua, (uint16_t)snum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(snum);
            size -= sizeof(snum);
          } else {
            size = 0;
          }
          break;
        }
        case 'i': {
          if (size >= sizeof(inum)) {
            std::memcpy(&inum, rp, sizeof(inum));
            lua_pushnumber(lua, (int32_t)inum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(inum);
            size -= sizeof(inum);
          } else {
            size = 0;
          }
          break;
        }
        case 'I': {
          if (size >= sizeof(inum)) {
            std::memcpy(&inum, rp, sizeof(inum));
            lua_pushnumber(lua, (uint32_t)inum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(inum);
            size -= sizeof(inum);
          } else {
            size = 0;
          }
          break;
        }
        case 'l': {
          if (size >= sizeof(lnum)) {
            std::memcpy(&lnum, rp, sizeof(lnum));
            lua_pushnumber(lua, (int64_t)lnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(lnum);
            size -= sizeof(lnum);
          } else {
            size = 0;
          }
          break;
        }
        case 'L': {
          if (size >= sizeof(lnum)) {
            std::memcpy(&lnum, rp, sizeof(lnum));
            lua_pushnumber(lua, (uint64_t)lnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(lnum);
            size -= sizeof(lnum);
          } else {
            size = 0;
          }
          break;
        }
        case 'f': case 'F': {
          if (size >= sizeof(fnum)) {
            std::memcpy(&fnum, rp, sizeof(fnum));
            lua_pushnumber(lua, (float)fnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(fnum);
            size -= sizeof(fnum);
          } else {
            size = 0;
          }
          break;
        }
        case 'd': case 'D': {
          if (size >= sizeof(dnum)) {
            std::memcpy(&dnum, rp, sizeof(dnum));
            lua_pushnumber(lua, (double)dnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(dnum);
            size -= sizeof(dnum);
          } else {
            size = 0;
          }
          break;
        }
        case 'n': {
          if (size >= sizeof(snum)) {
            std::memcpy(&snum, rp, sizeof(snum));
            snum = kc::ntoh16(snum);
            lua_pushnumber(lua, (uint16_t)snum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(snum);
            size -= sizeof(snum);
          } else {
            size = 0;
          }
          break;
        }
        case 'N': {
          if (size >= sizeof(inum)) {
            std::memcpy(&inum, rp, sizeof(inum));
            inum = kc::ntoh32(inum);
            lua_pushnumber(lua, (uint32_t)inum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(inum);
            size -= sizeof(inum);
          } else {
            size = 0;
          }
          break;
        }
        case 'M': {
          if (size >= sizeof(lnum)) {
            std::memcpy(&lnum, rp, sizeof(lnum));
            lnum = kc::ntoh64(lnum);
            lua_pushnumber(lua, (uint64_t)lnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(lnum);
            size -= sizeof(lnum);
          } else {
            size = 0;
          }
          break;
        }
        case 'w': case 'W': {
          wnum = 0;
          do {
            inum = *(unsigned char*)rp;
            wnum = wnum * 0x80 + (inum & 0x7f);
            rp++;
            size--;
          } while (inum >= 0x80 && size > 0);
          lua_pushnumber(lua, (uint64_t)wnum);
          lua_rawseti(lua, 3, eidx++);
          break;
        }
      }
    }
    format++;
    if (size < 1) break;
  }
  return 1;
}


/**
 * Implementation of split.
 */
static int kt_split(lua_State *lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1) throwinvarg(lua);
  size_t isiz;
  const char* ibuf = lua_tolstring(lua, 1, &isiz);
  if (!ibuf) throwinvarg(lua);
  const char* delims = argc > 1 ? lua_tostring(lua, 2) : NULL;
  lua_newtable(lua);
  int32_t lnum = 1;
  if (delims) {
    const char* str = ibuf;
    while (true) {
      const char* sp = str;
      while (*str != '\0' && !std::strchr(delims, *str)) {
        str++;
      }
      lua_pushlstring(lua, sp, str - sp);
      lua_rawseti(lua, -2, lnum++);
      if (*str == '\0') break;
      str++;
    }
  } else {
    const char* ptr = ibuf;
    int32_t size = isiz;
    while (size >= 0) {
      const char* rp = ptr;
      const char* ep = ptr + size;
      while (rp < ep) {
        if (*rp == '\0') break;
        rp++;
      }
      lua_pushlstring(lua, ptr, rp - ptr);
      lua_rawseti(lua, -2, lnum++);
      rp++;
      size -= rp - ptr;
      ptr = rp;
    }
  }
  return 1;
}


/**
 * Implementation of codec.
 */
static int kt_codec(lua_State *lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2) throwinvarg(lua);
  const char* mode = lua_tostring(lua, 1);
  size_t isiz;
  const char* ibuf = lua_tolstring(lua, 2, &isiz);
  if (!mode || !ibuf) throwinvarg(lua);
  char* obuf = NULL;
  size_t osiz = 0;
  if (*mode == '~') {
    mode++;
    if (!kc::stricmp(mode, "url")) {
      obuf = kc::urldecode(ibuf, &osiz);
    } else if (!kc::stricmp(mode, "base")) {
      obuf = kc::basedecode(ibuf, &osiz);
    } else if (!kc::stricmp(mode, "quote")) {
      obuf = kc::quotedecode(ibuf, &osiz);
    } else if (!kc::stricmp(mode, "hex")) {
      obuf = kc::hexdecode(ibuf, &osiz);
    } else if (!kc::stricmp(mode, "zlib")) {
      obuf = kc::ZLIB::decompress(ibuf, isiz, &osiz, kc::ZLIB::RAW);
    } else if (!kc::stricmp(mode, "deflate")) {
      obuf = kc::ZLIB::decompress(ibuf, isiz, &osiz, kc::ZLIB::DEFLATE);
    } else if (!kc::stricmp(mode, "gzip")) {
      obuf = kc::ZLIB::decompress(ibuf, isiz, &osiz, kc::ZLIB::GZIP);
    }
  } else {
    if (!kc::stricmp(mode, "url")) {
      obuf = kc::urlencode(ibuf, isiz);
      osiz = obuf ? std::strlen(obuf) : 0;
    } else if (!kc::stricmp(mode, "base")) {
      obuf = kc::baseencode(ibuf, isiz);
      osiz = obuf ? std::strlen(obuf) : 0;
    } else if (!kc::stricmp(mode, "quote")) {
      obuf = kc::quoteencode(ibuf, isiz);
      osiz = obuf ? std::strlen(obuf) : 0;
    } else if (!kc::stricmp(mode, "hex")) {
      obuf = kc::hexencode(ibuf, isiz);
      osiz = obuf ? std::strlen(obuf) : 0;
    } else if (!kc::stricmp(mode, "zlib")) {
      obuf = kc::ZLIB::compress(ibuf, isiz, &osiz, kc::ZLIB::RAW);
    } else if (!kc::stricmp(mode, "deflate")) {
      obuf = kc::ZLIB::compress(ibuf, isiz, &osiz, kc::ZLIB::DEFLATE);
    } else if (!kc::stricmp(mode, "gzip")) {
      obuf = kc::ZLIB::compress(ibuf, isiz, &osiz, kc::ZLIB::GZIP);
    }
  }
  if (obuf) {
    lua_pushlstring(lua, obuf, osiz);
    delete[] obuf;
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/**
 * Implementation of bit.
 */
static int kt_bit(lua_State *lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2) throwinvarg(lua);
  const char* mode = lua_tostring(lua, 1);
  uint32_t num = lua_tonumber(lua, 2);
  uint32_t aux = argc > 2 ? lua_tonumber(lua, 3) : 0;
  if (!mode) throwinvarg(lua);
  if (!kc::stricmp(mode, "and")) {
    num &= aux;
  } else if (!kc::stricmp(mode, "or")) {
    num |= aux;
  } else if (!kc::stricmp(mode, "xor")) {
    num ^= aux;
  } else if (!kc::stricmp(mode, "not")) {
    num = ~num;
  } else if (!kc::stricmp(mode, "left")) {
    num <<= aux;
  } else if (!kc::stricmp(mode, "right")) {
    num >>= aux;
  }
  lua_pushnumber(lua, num);
  return 1;
}


/**
 * Implementation of strstr.
 */
static int kt_strstr(lua_State *lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2) throwinvarg(lua);
  const char* str = lua_tostring(lua, 1);
  const char* pat = lua_tostring(lua, 2);
  if (!str || !pat) throwinvarg(lua);
  const char* alt = argc > 2 ? lua_tostring(lua, 3) : NULL;
  if (alt) {
    std::string xstr;
    size_t plen = std::strlen(pat);
    size_t alen = std::strlen(alt);
    if (plen > 0) {
      const char* pv;
      while ((pv = std::strstr(str, pat)) != NULL) {
        xstr.append(str, pv - str);
        xstr.append(alt, alen);
        str = pv + plen;
      }
    }
    xstr.append(str);
    lua_pushlstring(lua, xstr.data(), xstr.size());
  } else {
    const char* pv = std::strstr(str, pat);
    if (pv) {
      int32_t idx = pv - str + 1;
      lua_pushinteger(lua, idx);
    } else {
      lua_pushinteger(lua, 0);
    }
  }
  return 1;
}


/**
 * Implementation of strfwm.
 */
static int kt_strfwm(lua_State *lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2) throwinvarg(lua);
  const char* str = lua_tostring(lua, 1);
  const char* pat = lua_tostring(lua, 2);
  if (!str || !pat) throwinvarg(lua);
  lua_pushboolean(lua, kc::strfwm(str, pat));
  return 1;
}


/**
 * Implementation of strbwm.
 */
static int kt_strbwm(lua_State *lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2) throwinvarg(lua);
  const char* str = lua_tostring(lua, 1);
  const char* pat = lua_tostring(lua, 2);
  if (!str || !pat) throwinvarg(lua);
  lua_pushboolean(lua, kc::strbwm(str, pat));
  return 1;
}


/**
 * Define objects of the Error class.
 */
static void define_err(lua_State* lua) {
  lua_newtable(lua);
  setfielduint(lua, "SUCCESS", kc::BasicDB::Error::SUCCESS);
  setfielduint(lua, "NOIMPL", kc::BasicDB::Error::NOIMPL);
  setfielduint(lua, "INVALID", kc::BasicDB::Error::INVALID);
  setfielduint(lua, "NOREPOS", kc::BasicDB::Error::NOREPOS);
  setfielduint(lua, "NOPERM", kc::BasicDB::Error::NOPERM);
  setfielduint(lua, "BROKEN", kc::BasicDB::Error::BROKEN);
  setfielduint(lua, "DUPREC", kc::BasicDB::Error::DUPREC);
  setfielduint(lua, "NOREC", kc::BasicDB::Error::NOREC);
  setfielduint(lua, "LOGIC", kc::BasicDB::Error::LOGIC);
  setfielduint(lua, "SYSTEM", kc::BasicDB::Error::SYSTEM);
  setfielduint(lua, "MISC", kc::BasicDB::Error::MISC);
  setfieldfunc(lua, "new", err_new);
  lua_newtable(lua);
  setfieldfunc(lua, "__tostring", err_tostring);
  lua_setfield(lua, -2, "meta_");
  lua_setfield(lua, 1, "Error");
}


/**
 * Implementation of new.
 */
static int err_new(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 3) throwinvarg(lua);
  lua_newtable(lua);
  if (argc > 2 && lua_isnumber(lua, 2) && lua_isstring(lua, 3)) {
    setfieldvalue(lua, "code_", 2);
    setfieldvalue(lua, "message_", 3);
  } else {
    setfielduint(lua, "code_", 0);
    setfieldstr(lua, "message_", "error");
  }
  setfieldfunc(lua, "set", err_set);
  setfieldfunc(lua, "code", err_code);
  setfieldfunc(lua, "name", err_name);
  setfieldfunc(lua, "message", err_message);
  lua_getfield(lua, 1, "meta_");
  lua_setmetatable(lua, -2);
  return 1;
}


/**
 * Implementation of __tostring.
 */
static int err_tostring(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "code_");
  uint32_t code = lua_tonumber(lua, -1);
  lua_getfield(lua, 1, "message_");
  const char* message = lua_tostring(lua, -1);
  const char* name = kc::BasicDB::Error::codename((kc::BasicDB::Error::Code)code);
  lua_pushfstring(lua, "%s: %s", name, message);
  return 1;
}


/**
 * Implementation of set.
 */
static int err_set(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2 && lua_isnumber(lua, 2) && lua_isstring(lua, 3)) {
    lua_pushvalue(lua, 2);
    lua_setfield(lua, 1, "code_");
    lua_pushvalue(lua, 3);
    lua_setfield(lua, 1, "message_");
  }
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of code.
 */
static int err_code(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "code_");
  return 1;
}


/**
 * Implementation of name.
 */
static int err_name(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "code_");
  uint32_t code = lua_tonumber(lua, -1);
  const char* name = kc::BasicDB::Error::codename((kc::BasicDB::Error::Code)code);
  lua_pushstring(lua, name);
  return 1;
}


/**
 * Implementation of message.
 */
static int err_message(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "message_");
  return 1;
}


/**
 * Define objects of the Visitor class.
 */
static void define_vis(lua_State* lua) {
  lua_newtable(lua);
  lua_pushlightuserdata(lua, (void*)kt::TimedDB::Visitor::NOP);
  lua_setfield(lua, -2, "NOP");
  lua_pushlightuserdata(lua, (void*)kt::TimedDB::Visitor::REMOVE);
  lua_setfield(lua, -2, "REMOVE");
  setfieldfunc(lua, "new", vis_new);
  lua_newtable(lua);
  lua_setfield(lua, -2, "meta_");
  lua_setfield(lua, 1, "Visitor");
}


/**
 * Implementation of new.
 */
static int vis_new(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_newtable(lua);
  setfieldfunc(lua, "visit_full", vis_visit_full);
  setfieldfunc(lua, "visit_empty", vis_visit_empty);
  lua_getfield(lua, 1, "meta_");
  lua_setmetatable(lua, -2);
  return 1;
}


/**
 * Implementation of visit_full.
 */
static int vis_visit_full(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of visit_empty.
 */
static int vis_visit_empty(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_pushnil(lua);
  return 1;
}


/**
 * Define objects of the FileProcessor class.
 */
static void define_fproc(lua_State* lua) {
  lua_newtable(lua);
  setfieldfunc(lua, "new", fproc_new);
  lua_newtable(lua);
  lua_setfield(lua, -2, "meta_");
  lua_setfield(lua, 1, "FileProcessor");
}


/**
 * Implementation of new.
 */
static int fproc_new(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_newtable(lua);
  setfieldfunc(lua, "process", fproc_process);
  lua_getfield(lua, 1, "meta_");
  lua_setmetatable(lua, -2);
  return 1;
}


/**
 * Implementation of process.
 */
static int fproc_process(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 4 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_pushboolean(lua, true);
  return 1;
}


/**
 * Define objects of the Cursor class.
 */
static void define_cur(lua_State* lua) {
  lua_newtable(lua);
  setfieldfunc(lua, "new", cur_new);
  lua_newtable(lua);
  setfieldfunc(lua, "__tostring", cur_tostring);
  setfieldfunc(lua, "__call", cur_call);
  lua_setfield(lua, -2, "meta_");
  lua_setfield(lua, 1, "Cursor");
}


/**
 * Implementation of new.
 */
static int cur_new(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1) || !lua_istable(lua, 2)) throwinvarg(lua);
  lua_getfield(lua, 2, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  lua_newtable(lua);
  g_curbur.sweap();
  SoftCursor* cur = (SoftCursor*)lua_newuserdata(lua, sizeof(*cur));
  cur->cur = db->db->cursor();
  lua_newtable(lua);
  setfieldfunc(lua, "__gc", cur_gc);
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, "cur_ptr_");
  lua_pushvalue(lua, 2);
  lua_setfield(lua, -2, "db_");
  setfieldfunc(lua, "disable", cur_disable);
  setfieldfunc(lua, "accept", cur_accept);
  setfieldfunc(lua, "set_value", cur_set_value);
  setfieldfunc(lua, "remove", cur_remove);
  setfieldfunc(lua, "get_key", cur_get_key);
  setfieldfunc(lua, "get_value", cur_get_value);
  setfieldfunc(lua, "get", cur_get);
  setfieldfunc(lua, "jump", cur_jump);
  setfieldfunc(lua, "jump_back", cur_jump_back);
  setfieldfunc(lua, "step", cur_step);
  setfieldfunc(lua, "step_back", cur_step_back);
  setfieldfunc(lua, "db", cur_db);
  setfieldfunc(lua, "error", cur_error);
  lua_getfield(lua, 1, "meta_");
  lua_setmetatable(lua, -2);
  return 1;
}


/**
 * Implementation of __gc.
 */
static int cur_gc(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_isuserdata(lua, 1)) return 0;
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, 1);
  if (!cur) return 0;
  if (cur->cur) g_curbur.deposit(cur->cur);
  return 0;
}


/**
 * Implementation of __tostring.
 */
static int cur_tostring(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) {
    lua_pushstring(lua, "(disabled)");
    return 1;
  }
  kt::TimedDB* db = cur->cur->db();
  std::string path = db->path();
  if (path.size() < 1) path = "(nil)";
  std::string str = kc::strprintf("%s: ", path.c_str());
  size_t ksiz;
  char* kbuf = cur->cur->get_key(&ksiz);
  if (kbuf) {
    str.append(kbuf, ksiz);
    delete[] kbuf;
  } else {
    str.append("(nil)");
  }
  lua_pushstring(lua, str.c_str());
  return 1;
}


/**
 * Implementation of __call.
 */
static int cur_call(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  const char* vbuf;
  size_t ksiz, vsiz;
  int64_t xt;
  char* kbuf = cur->cur->get(&ksiz, &vbuf, &vsiz, &xt, true);
  if (kbuf) {
    lua_pushlstring(lua, kbuf, ksiz);
    lua_pushlstring(lua, vbuf, vsiz);
    delete[] kbuf;
    return 2;
  }
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of disable.
 */
static int cur_disable(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  delete cur->cur;
  cur->cur = NULL;
  lua_pushnil(lua);
  lua_setfield(lua, 1, "cur_ptr_");
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of accept.
 */
static int cur_accept(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool writable = argc > 2 ? lua_toboolean(lua, 3) : true;
  bool step = argc > 3 ? lua_toboolean(lua, 4) : false;
  bool rv;
  if (lua_istable(lua, 2) || lua_isfunction(lua, 2)) {
    lua_pushvalue(lua, 2);
    SoftVisitor visitor(lua, writable);
    rv = cur->cur->accept(&visitor, writable, step);
  } else {
    throwinvarg(lua);
    rv = false;
  }
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of set_value.
 */
static int cur_set_value(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  size_t vsiz;
  const char* vbuf = lua_tolstring(lua, 2, &vsiz);
  if (!cur || !vsiz) throwinvarg(lua);
  int64_t xt = argc > 2 && !lua_isnil(lua, 3) ? lua_tonumber(lua, 3) : INT64_MAX;
  bool step = argc > 3 ? lua_toboolean(lua, 4) : false;
  bool rv = cur->cur->set_value(vbuf, vsiz, step, xt);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of remove.
 */
static int cur_remove(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool rv = cur->cur->remove();
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of get_key.
 */
static int cur_get_key(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool step = argc > 1 ? lua_toboolean(lua, 2) : false;
  size_t ksiz;
  char* kbuf = cur->cur->get_key(&ksiz, step);
  if (kbuf) {
    lua_pushlstring(lua, kbuf, ksiz);
    delete[] kbuf;
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/**
 * Implementation of get_value.
 */
static int cur_get_value(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool step = argc > 1 ? lua_toboolean(lua, 2) : false;
  size_t vsiz;
  char* vbuf = cur->cur->get_value(&vsiz, step);
  if (vbuf) {
    lua_pushlstring(lua, vbuf, vsiz);
    delete[] vbuf;
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/**
 * Implementation of get.
 */
static int cur_get(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool step = argc > 1 ? lua_toboolean(lua, 2) : false;
  const char* vbuf;
  size_t ksiz, vsiz;
  int64_t xt;
  char* kbuf = cur->cur->get(&ksiz, &vbuf, &vsiz, &xt, step);
  if (kbuf) {
    lua_pushlstring(lua, kbuf, ksiz);
    lua_pushlstring(lua, vbuf, vsiz);
    lua_pushnumber(lua, xt < kt::TimedDB::XTMAX ? xt : kt::TimedDB::XTMAX);
    delete[] kbuf;
    return 3;
  }
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of jump.
 */
static int cur_jump(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool rv;
  if (argc > 1) {
    size_t ksiz;
    const char* kbuf = lua_tolstring(lua, 2, &ksiz);
    if (!kbuf) throwinvarg(lua);
    rv = cur->cur->jump(kbuf, ksiz);
  } else {
    rv = cur->cur->jump();
  }
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of jump_back.
 */
static int cur_jump_back(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool rv;
  if (argc > 1) {
    size_t ksiz;
    const char* kbuf = lua_tolstring(lua, 2, &ksiz);
    if (!kbuf) throwinvarg(lua);
    rv = cur->cur->jump_back(kbuf, ksiz);
  } else {
    rv = cur->cur->jump_back();
  }
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of step.
 */
static int cur_step(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool rv = cur->cur->step();
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of step_back.
 */
static int cur_step_back(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  bool rv = cur->cur->step_back();
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of db.
 */
static int cur_db(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  lua_getfield(lua, 1, "db_");
  return 1;
}


/**
 * Implementation of error.
 */
static int cur_error(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_ptr_");
  SoftCursor* cur = (SoftCursor*)lua_touserdata(lua, -1);
  if (!cur) throwinvarg(lua);
  lua_getfield(lua, 1, "db_");
  lua_getfield(lua, -1, "error");
  lua_pushvalue(lua, -2);
  lua_call(lua, 1, 1);
  return 1;
}


/**
 * Define objects of the DB class.
 */
static void define_db(lua_State* lua) {
  lua_newtable(lua);
  setfielduint(lua, "OREADER", kc::BasicDB::OREADER);
  setfielduint(lua, "OWRITER", kc::BasicDB::OWRITER);
  setfielduint(lua, "OCREATE", kc::BasicDB::OCREATE);
  setfielduint(lua, "OTRUNCATE", kc::BasicDB::OTRUNCATE);
  setfielduint(lua, "OAUTOTRAN", kc::BasicDB::OAUTOTRAN);
  setfielduint(lua, "OAUTOSYNC", kc::BasicDB::OAUTOSYNC);
  setfielduint(lua, "ONOLOCK", kc::BasicDB::ONOLOCK);
  setfielduint(lua, "OTRYLOCK", kc::BasicDB::OTRYLOCK);
  setfielduint(lua, "ONOREPAIR", kc::BasicDB::ONOREPAIR);
  setfieldfunc(lua, "new", db_new);
  setfieldfunc(lua, "new_ptr", db_new_ptr);
  setfieldfunc(lua, "delete_ptr", db_delete_ptr);
  setfieldfunc(lua, "process", db_process);
  lua_newtable(lua);
  setfieldfunc(lua, "__tostring", db_tostring);
  setfieldfunc(lua, "__index", db_index);
  setfieldfunc(lua, "__newindex", db_newindex);
  lua_setfield(lua, -2, "meta_");
  lua_setfield(lua, 1, "DB");
}


/**
 * Implementation of new.
 */
static int db_new(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_newtable(lua);
  SoftDB* db = (SoftDB*)lua_newuserdata(lua, sizeof(*db));
  if (argc > 1 && lua_islightuserdata(lua, 2)) {
    db->db = (kt::TimedDB*)lua_touserdata(lua, 2);
    db->light = true;
  } else {
    db->db = new kt::TimedDB;
    db->light = false;
  }
  lua_newtable(lua);
  setfieldfunc(lua, "__gc", db_gc);
  lua_setmetatable(lua, -2);
  lua_setfield(lua, -2, "db_ptr_");
  lua_getglobal(lua, "__kyototycoon__");
  lua_getfield(lua, -1, "Error");
  lua_setfield(lua, -3, "err_");
  lua_getfield(lua, -1, "Cursor");
  lua_setfield(lua, -3, "cur_");
  lua_pop(lua, 1);
  setfieldfunc(lua, "error", db_error);
  setfieldfunc(lua, "open", db_open);
  setfieldfunc(lua, "close", db_close);
  setfieldfunc(lua, "accept", db_accept);
  setfieldfunc(lua, "iterate", db_iterate);
  setfieldfunc(lua, "set", db_set);
  setfieldfunc(lua, "add", db_add);
  setfieldfunc(lua, "replace", db_replace);
  setfieldfunc(lua, "append", db_append);
  setfieldfunc(lua, "increment", db_increment);
  setfieldfunc(lua, "increment_double", db_increment_double);
  setfieldfunc(lua, "cas", db_cas);
  setfieldfunc(lua, "remove", db_remove);
  setfieldfunc(lua, "get", db_get);
  setfieldfunc(lua, "clear", db_clear);
  setfieldfunc(lua, "synchronize", db_synchronize);
  setfieldfunc(lua, "copy", db_copy);
  setfieldfunc(lua, "begin_transaction", db_begin_transaction);
  setfieldfunc(lua, "end_transaction", db_end_transaction);
  setfieldfunc(lua, "transaction", db_transaction);
  setfieldfunc(lua, "dump_snapshot", db_dump_snapshot);
  setfieldfunc(lua, "load_snapshot", db_load_snapshot);
  setfieldfunc(lua, "count", db_count);
  setfieldfunc(lua, "size", db_size);
  setfieldfunc(lua, "path", db_path);
  setfieldfunc(lua, "status", db_status);
  setfieldfunc(lua, "cursor", db_cursor);
  setfieldfunc(lua, "cursor_process", db_cursor_process);
  setfieldfunc(lua, "pairs", db_pairs);
  lua_getfield(lua, 1, "meta_");
  lua_setmetatable(lua, -2);
  return 1;
}


/**
 * Implementation of __gc.
 */
static int db_gc(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_isuserdata(lua, 1)) return 0;
  SoftDB* db = (SoftDB*)lua_touserdata(lua, 1);
  if (!db) return 0;
  if (!db->light) delete db->db;
  return 0;
}


/**
 * Implementation of __tostring.
 */
static int db_tostring(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  std::string path = db->db->path();
  if (path.size() < 1) path = "(nil)";
  std::string str = kc::strprintf("%s: %lld: %lld", path.c_str(),
                                  (long long)db->db->count(), (long long)db->db->size());
  lua_pushstring(lua, str.c_str());
  return 1;
}


/**
 * Implementation of __index.
 */
static int db_index(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  if (!db || !kbuf) throwinvarg(lua);
  size_t vsiz;
  char* vbuf = db->db->get(kbuf, ksiz, &vsiz);
  if (vbuf) {
    lua_pushlstring(lua, vbuf, vsiz);
    delete[] vbuf;
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/**
 * Implementation of __newindex.
 */
static int db_newindex(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char* vbuf = lua_tolstring(lua, 3, &vsiz);
  if (!db || !kbuf) throwinvarg(lua);
  bool rv;
  if (vbuf) {
    rv = db->db->set(kbuf, ksiz, vbuf, vsiz);
  } else {
    rv = db->db->remove(kbuf, ksiz);
  }
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of new_ptr.
 */
static int db_new_ptr(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  kt::TimedDB* db = new kt::TimedDB;
  lua_pushlightuserdata(lua, (void*)db);
  return 1;
}


/**
 * Implementation of delete_ptr.
 */
static int db_delete_ptr(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (lua_islightuserdata(lua, 2)) {
    kt::TimedDB* db = (kt::TimedDB*)lua_touserdata(lua, 2);
    delete db;
  }
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of process.
 */
static int db_process(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  if (!lua_isfunction(lua, 2)) throwinvarg(lua);
  lua_getfield(lua, 1, "new");
  lua_pushvalue(lua, 1);
  lua_call(lua, 1, 1);
  int32_t objidx = lua_gettop(lua);
  lua_getfield(lua, objidx, "open");
  lua_pushvalue(lua, objidx);
  if (argc > 2) lua_pushvalue(lua, 3);
  if (argc > 3) lua_pushvalue(lua, 4);
  lua_call(lua, argc - 1, 1);
  if (!lua_toboolean(lua, -1)) {
    lua_getfield(lua, objidx, "error");
    lua_pushvalue(lua, objidx);
    lua_call(lua, 1, 1);
    return 1;
  }
  lua_pushvalue(lua, 2);
  lua_pushvalue(lua, objidx);
  int32_t erridx = 0;
  if (lua_pcall(lua, 1, 0, 0) != 0) erridx = lua_gettop(lua);
  lua_getfield(lua, objidx, "close");
  lua_pushvalue(lua, objidx);
  lua_call(lua, 1, 1);
  if (erridx > 0) {
    lua_pushvalue(lua, erridx);
    lua_error(lua);
  }
  if (!lua_toboolean(lua, -1)) {
    lua_getfield(lua, objidx, "error");
    lua_pushvalue(lua, objidx);
    lua_call(lua, 1, 1);
    return 1;
  }
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of error.
 */
static int db_error(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  kc::BasicDB::Error err = db->db->error();
  lua_getfield(lua, 1, "err_");
  lua_getfield(lua, -1, "new");
  lua_pushvalue(lua, -2);
  lua_pushinteger(lua, err.code());
  lua_pushstring(lua, err.message());
  lua_call(lua, 3, 1);
  return 1;
}


/**
 * Implementation of open.
 */
static int db_open(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 3) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  const char* path = "*";
  if (argc > 1 && lua_isstring(lua, 2)) path = lua_tostring(lua, 2);
  uint32_t mode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE;
  if (argc > 2 && lua_isnumber(lua, 3)) mode = lua_tonumber(lua, 3);
  bool rv = db->db->open(path, mode);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of close.
 */
static int db_close(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  g_curbur.sweap();
  bool rv = db->db->close();
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of accept.
 */
static int db_accept(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  if (!db || !kbuf) throwinvarg(lua);
  bool writable = argc > 3 ? lua_toboolean(lua, 4) : true;
  bool rv;
  if (lua_istable(lua, 3) || lua_isfunction(lua, 3)) {
    lua_pushvalue(lua, 3);
    SoftVisitor visitor(lua, writable);
    rv = db->db->accept(kbuf, ksiz, &visitor, writable);
  } else {
    throwinvarg(lua);
    rv = false;
  }
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of iterate.
 */
static int db_iterate(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 3) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  bool writable = argc > 2 ? lua_toboolean(lua, 3) : true;
  bool rv;
  if (lua_istable(lua, 2) || lua_isfunction(lua, 2)) {
    lua_pushvalue(lua, 2);
    SoftVisitor visitor(lua, writable);
    rv = db->db->iterate(&visitor, writable);
  } else {
    throwinvarg(lua);
    rv = false;
  }
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of set.
 */
static int db_set(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char* vbuf = lua_tolstring(lua, 3, &vsiz);
  if (!db || !kbuf || !vbuf) throwinvarg(lua);
  int64_t xt = argc > 3 && !lua_isnil(lua, 4) ? lua_tonumber(lua, 4) : INT64_MAX;
  bool rv = db->db->set(kbuf, ksiz, vbuf, vsiz, xt);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of add.
 */
static int db_add(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char* vbuf = lua_tolstring(lua, 3, &vsiz);
  if (!db || !kbuf || !vbuf) throwinvarg(lua);
  int64_t xt = argc > 3 && !lua_isnil(lua, 4) ? lua_tonumber(lua, 4) : INT64_MAX;
  bool rv = db->db->add(kbuf, ksiz, vbuf, vsiz, xt);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of replace.
 */
static int db_replace(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char* vbuf = lua_tolstring(lua, 3, &vsiz);
  if (!db || !kbuf || !vbuf) throwinvarg(lua);
  int64_t xt = argc > 3 && !lua_isnil(lua, 4) ? lua_tonumber(lua, 4) : INT64_MAX;
  bool rv = db->db->replace(kbuf, ksiz, vbuf, vsiz, xt);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of append.
 */
static int db_append(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 3 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t vsiz;
  const char* vbuf = lua_tolstring(lua, 3, &vsiz);
  if (!db || !kbuf || !vbuf) throwinvarg(lua);
  int64_t xt = argc > 3 && !lua_isnil(lua, 4) ? lua_tonumber(lua, 4) : INT64_MAX;
  bool rv = db->db->append(kbuf, ksiz, vbuf, vsiz, xt);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of increment.
 */
static int db_increment(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  if (!db || !kbuf) throwinvarg(lua);
  int64_t num = argc > 2 ? lua_tonumber(lua, 3) : 0;
  int64_t xt = argc > 3 && !lua_isnil(lua, 4) ? lua_tonumber(lua, 4) : INT64_MAX;
  num = db->db->increment(kbuf, ksiz, num, xt);
  if (num == INT64_MIN) {
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/**
 * Implementation of increment_double.
 */
static int db_increment_double(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 4) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  if (!db || !kbuf) throwinvarg(lua);
  double num = argc > 2 ? lua_tonumber(lua, 3) : 0;
  int64_t xt = argc > 3 && !lua_isnil(lua, 4) ? lua_tonumber(lua, 4) : INT64_MAX;
  num = db->db->increment_double(kbuf, ksiz, num, xt);
  if (kc::chknan(num)) {
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, num);
  }
  return 1;
}


/**
 * Implementation of cas.
 */
static int db_cas(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 4 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 5) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  size_t ovsiz;
  const char* ovbuf = lua_tolstring(lua, 3, &ovsiz);
  size_t nvsiz;
  const char* nvbuf = lua_tolstring(lua, 4, &nvsiz);
  if (!db || !kbuf) throwinvarg(lua);
  int64_t xt = argc > 4 && !lua_isnil(lua, 5) ? lua_tonumber(lua, 5) : INT64_MAX;
  bool rv = db->db->cas(kbuf, ksiz, ovbuf, ovsiz, nvbuf, nvsiz, xt);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of remove.
 */
static int db_remove(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  if (!db || !kbuf) throwinvarg(lua);
  bool rv = db->db->remove(kbuf, ksiz);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of get.
 */
static int db_get(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  size_t ksiz;
  const char* kbuf = lua_tolstring(lua, 2, &ksiz);
  if (!db || !kbuf) throwinvarg(lua);
  size_t vsiz;
  int64_t xt;
  char* vbuf = db->db->get(kbuf, ksiz, &vsiz, &xt);
  if (vbuf) {
    lua_pushlstring(lua, vbuf, vsiz);
    lua_pushnumber(lua, xt < kt::TimedDB::XTMAX ? xt : kt::TimedDB::XTMAX);
    delete[] vbuf;
    return 2;
  }
  lua_pushnil(lua);
  return 1;
}


/**
 * Implementation of clear.
 */
static int db_clear(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  bool rv = db->db->clear();
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of synchronize.
 */
static int db_synchronize(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 3) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  bool hard = argc > 1 ? lua_toboolean(lua, 2) : false;
  bool rv;
  if (argc > 2 && (lua_istable(lua, 3) || lua_isfunction(lua, 3))) {
    lua_pushvalue(lua, 3);
    SoftFileProcessor proc(lua);
    rv = db->db->synchronize(hard, &proc);
  } else {
    rv = db->db->synchronize(hard, NULL);
  }
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of copy.
 */
static int db_copy(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  const char* dest = lua_tostring(lua, 2);
  if (!db || !dest) throwinvarg(lua);
  bool rv = db->db->copy(dest);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of begin_transaction.
 */
static int db_begin_transaction(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  bool hard = argc > 1 ? lua_toboolean(lua, 2) : false;
  bool rv = db->db->begin_transaction(hard);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of end_transaction.
 */
static int db_end_transaction(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 2) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  bool commit = argc > 1 ? lua_toboolean(lua, 2) : true;
  bool rv = db->db->end_transaction(commit);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of transaction.
 */
static int db_transaction(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc < 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (argc > 3) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  bool hard = argc > 2 ? lua_toboolean(lua, 3) : false;
  lua_getfield(lua, 1, "begin_transaction");
  lua_pushvalue(lua, 1);
  lua_pushboolean(lua, hard);
  lua_call(lua, 2, 1);
  if (!lua_toboolean(lua, -1)) return 1;
  lua_pushvalue(lua, 2);
  int32_t erridx = 0;
  if (lua_pcall(lua, 0, 1, 0) != 0) erridx = lua_gettop(lua);
  bool commit = erridx == 0 && lua_toboolean(lua, -1);
  lua_getfield(lua, 1, "end_transaction");
  lua_pushvalue(lua, 1);
  lua_pushboolean(lua, commit);
  lua_call(lua, 2, 1);
  if (erridx > 0) {
    lua_pushvalue(lua, erridx);
    lua_error(lua);
  }
  return 1;
}


/**
 * Implementation of dump_snapshot.
 */
static int db_dump_snapshot(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  const char* dest = lua_tostring(lua, 2);
  if (!db || !dest) throwinvarg(lua);
  bool rv = db->db->dump_snapshot(dest);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of load_snapshot.
 */
static int db_load_snapshot(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  const char* src = lua_tostring(lua, 2);
  if (!db || !src) throwinvarg(lua);
  bool rv = db->db->load_snapshot(src);
  lua_pushboolean(lua, rv);
  return 1;
}


/**
 * Implementation of count.
 */
static int db_count(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  int64_t count = db->db->count();
  lua_pushnumber(lua, count);
  return 1;
}


/**
 * Implementation of size.
 */
static int db_size(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  int64_t size = db->db->size();
  lua_pushnumber(lua, size);
  return 1;
}


/**
 * Implementation of path.
 */
static int db_path(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  const std::string& path = db->db->path();
  if (path.size() > 0) {
    lua_pushstring(lua, path.c_str());
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/**
 * Implementation of status.
 */
static int db_status(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  StringMap status;
  bool rv = db->db->status(&status);
  if (rv) {
    lua_newtable(lua);
    StringMap::const_iterator it = status.begin();
    StringMap::const_iterator itend = status.end();
    while (it != itend) {
      lua_pushstring(lua, it->second.c_str());
      lua_setfield(lua, -2, it->first.c_str());
      it++;
    }
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/**
 * Implementation of cursor.
 */
static int db_cursor(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_");
  lua_getfield(lua, -1, "new");
  lua_getfield(lua, 1, "cur_");
  lua_pushvalue(lua, 1);
  lua_call(lua, 2, 1);
  return 1;
}


/**
 * Implementation of cursor_process.
 */
static int db_cursor_process(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_istable(lua, 1)) throwinvarg(lua);
  if (!lua_isfunction(lua, 2)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_");
  lua_getfield(lua, -1, "new");
  lua_getfield(lua, 1, "cur_");
  lua_pushvalue(lua, 1);
  lua_call(lua, 2, 1);
  int32_t objidx = lua_gettop(lua);
  lua_pushvalue(lua, 2);
  lua_pushvalue(lua, objidx);
  int32_t erridx = 0;
  if (lua_pcall(lua, 1, 0, 0) != 0) erridx = lua_gettop(lua);
  lua_getfield(lua, objidx, "disable");
  lua_pushvalue(lua, objidx);
  lua_call(lua, 1, 0);
  if (erridx > 0) {
    lua_pushvalue(lua, erridx);
    lua_error(lua);
  }
  return 0;
}


/**
 * Implementation of pairs.
 */
static int db_pairs(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 1 || !lua_istable(lua, 1)) throwinvarg(lua);
  lua_getfield(lua, 1, "db_ptr_");
  SoftDB* db = (SoftDB*)lua_touserdata(lua, -1);
  if (!db) throwinvarg(lua);
  lua_getfield(lua, 1, "cur_");
  lua_getfield(lua, -1, "new");
  lua_getfield(lua, 1, "cur_");
  lua_pushvalue(lua, 1);
  lua_call(lua, 2, 1);
  lua_getfield(lua, -1, "jump");
  lua_pushvalue(lua, -2);
  lua_call(lua, 1, 0);
  lua_pushvalue(lua, 1);
  lua_pushnil(lua);
  return 3;
}


/**
 * Implementation of log.
 */
static int serv_log(lua_State* lua) {
  int32_t argc = lua_gettop(lua);
  if (argc != 2 || !lua_isstring(lua, 1)) throwinvarg(lua);
  lua_getglobal(lua, "__kyototycoon__");
  lua_getfield(lua, -1, "__serv__");
  kt::RPCServer* serv = (kt::RPCServer*)lua_touserdata(lua, -1);
  const char* kstr = lua_tostring(lua, 1);
  const char* message = lua_tostring(lua, 2);
  if (kstr && message) {
    kt::RPCServer::Logger::Kind kind = kt::RPCServer::Logger::DEBUG;
    if (!kc::stricmp(kstr, "info")) {
      kind = kt::RPCServer::Logger::INFO;
    } else if (!kc::stricmp(kstr, "system")) {
      kind = kt::RPCServer::Logger::SYSTEM;
    } else if (!kc::stricmp(kstr, "error")) {
      kind = kt::RPCServer::Logger::ERROR;
    }
    serv->log(kind, "[SCRIPT]: %s", message);
  }
  return 0;
}


#else


/**
 * ScriptProcessor internal.
 */
struct ScriptProcessorCore {
  std::string path;
  int32_t thid;
  kt::RPCServer* serv;
  kt::TimedDB* dbs;
  int32_t dbnum;
  const std::map<std::string, int32_t>* dbmap;
};


/**
 * Default constructor.
 */
ScriptProcessor::ScriptProcessor() {
  _assert_(true);
  ScriptProcessorCore* core = new ScriptProcessorCore;
  core->thid = 0;
  core->serv = NULL;
  core->dbs = NULL;
  core->dbnum = 0;
  core->dbmap = NULL;
  opq_ = core;
}


/**
 * Destructor.
 */
ScriptProcessor::~ScriptProcessor() {
  _assert_(true);
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  delete core;
}


/**
 * Set domain-specific resources.
 */
bool ScriptProcessor::set_resources(int32_t thid, kt::RPCServer* serv,
                                    kt::TimedDB* dbs, int32_t dbnum,
                                    const std::map<std::string, int32_t>* dbmap) {
  _assert_(serv && dbs && dbnum >= 0 && dbmap);
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  core->thid = thid;
  core->serv = serv;
  core->dbs = dbs;
  core->dbnum = dbnum;
  core->dbmap = dbmap;
  return true;
}


/**
 * Load a script file.
 */
bool ScriptProcessor::load(const std::string& path) {
  _assert_(true);
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  core->path = path;
  return true;
}


/**
 * Call a procedure.
 */
kt::RPCClient::ReturnValue ScriptProcessor::call(const std::string& name,
                                                 const std::map<std::string, std::string>& inmap,
                                                 std::map<std::string, std::string>& outmap) {
  _assert_(true);
  ScriptProcessorCore* core = (ScriptProcessorCore*)opq_;
  kt::RPCClient::ReturnValue rv;
  if (name == "echo") {
    std::string keys;
    std::map<std::string, std::string>::const_iterator it = inmap.begin();
    std::map<std::string, std::string>::const_iterator itend = inmap.end();
    while (it != itend) {
      if (!keys.empty()) keys.append(",");
      keys.append(it->first);
      it++;
    }
    core->serv->log(kt::RPCServer::Logger::DEBUG, "[SCRIPT]: %s: thid=%d inmap=%s",
                    name.c_str(), core->thid, keys.c_str());
    outmap.insert(inmap.begin(), inmap.end());
    rv = kt::RPCClient::RVSUCCESS;
  } else {
    rv = kt::RPCClient::RVENOIMPL;
  }
  return rv;
}


#endif


// END OF FILE
