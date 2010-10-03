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
#include <ktutil.h>
#include <ktsocket.h>
#include <ktthserv.h>
#include <kthttp.h>
#include <ktrpc.h>
#include <kttimeddb.h>

namespace kyototycoon {                  // common namespace


/**
 * Remote database.
 * @note This class is a concrete class to access remote database servers.  This class can be
 * inherited but overwriting methods is forbidden.  Before every database operation, it is
 * necessary to call the RemoteDB::open method in order to connect to a database server.  To
 * avoid resource starvation it is important to close every database file by the RemoteDB::close
 * method when the connection is no longer in use.  Although all methods of this class are
 * thread-safe, its instance does not have mutual exclusion mechanism.  So, multiple threads
 * must not share the same instance and they must use their own respective instances.
 */
class RemoteDB {
public:
  class Cursor;
  class Error;
private:
  /** An alias of list of cursors. */
  typedef std::list<Cursor*> CursorList;
public:
  /**
   * Cursor to indicate a record.
   */
  class Cursor {
    friend class RemoteDB;
  public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(RemoteDB* db) : db_(db), id_(0) {
      _assert_(db);
      uint64_t uid = (((uint64_t)(intptr_t)db_ >> 8) << 16) ^ ((uint64_t)(intptr_t)this >> 8);
      uid ^= ((uint64_t)(kc::time() * 65536)) << 24;
      id_ = ((uid << 16) & (INT64_MAX >> 4)) + (++db->curcnt_);
      db_->curs_.push_back(this);
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      _assert_(true);
      if (!db_) return;
      std::map<std::string, std::string> inmap;
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      db_->rpc_.call("cur_delete", &inmap, &outmap);
      db_->curs_.remove(this);
    }
    /**
     * Jump the cursor to the first record for forward scan.
     * @return true on success, or false on failure.
     */
    bool jump() {
      _assert_(true);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Jump the cursor to a record for forward scan.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     */
    bool jump(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      inmap["key"] = std::string(kbuf, ksiz);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Jump the cursor to a record for forward scan.
     * @note Equal to the original Cursor::jump method except that the parameter is std::string.
     */
    bool jump(const std::string& key) {
      _assert_(true);
      return jump(key.c_str(), key.size());
    }
    /**

     * Jump the cursor to the last record for backward scan.
     * @return true on success, or false on failure.
     * @note This method is dedicated to tree databases.  Some database types, especially hash
     * databases, may provide a dummy implementation.
     */
    bool jump_back() {
      _assert_(true);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump_back", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Jump the cursor to a record for backward scan.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     * @note This method is dedicated to tree databases.  Some database types, especially hash
     * databases, will provide a dummy implementation.
     */
    bool jump_back(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      inmap["key"] = std::string(kbuf, ksiz);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump_back", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Jump the cursor to a record for backward scan.
     * @note Equal to the original Cursor::jump_back method except that the parameter is
     * std::string.
     */
    bool jump_back(const std::string& key) {
      _assert_(true);
      return jump_back(key.c_str(), key.size());
    }
    /**
     * Step the cursor to the next record.
     * @return true on success, or false on failure.
     */
    bool step() {
      _assert_(true);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_step", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Step the cursor to the previous record.
     * @return true on success, or false on failure.
     * @note This method is dedicated to tree databases.  Some database types, especially hash
     * databases, may provide a dummy implementation.
     */
    bool step_back() {
      _assert_(true);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_step_back", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Set the value of the current record.
     * @param vbuf the pointer to the value region.
     * @param vsiz the size of the value region.
     * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
     * is treated as the epoch time.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return true on success, or false on failure.
     */
    bool set_value(const char* vbuf, size_t vsiz, int64_t xt = INT64_MAX, bool step = false) {
      _assert_(vbuf && vsiz <= kc::MEMMAXSIZ);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      inmap["value"] = std::string(vbuf, vsiz);
      if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_set_value", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Set the value of the current record.
     * @note Equal to the original Cursor::set_value method except that the parameter is
     * std::string.
     */
    bool set_value_str(const std::string& value, int64_t xt = INT64_MAX, bool step = false) {
      _assert_(true);
      return set_value(value.c_str(), value.size(), xt, step);
    }
    /**
     * Remove the current record.
     * @return true on success, or false on failure.
     * @note If no record corresponds to the key, false is returned.  The cursor is moved to the
     * next record implicitly.
     */
    bool remove() {
      _assert_(true);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_remove", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
    }
    /**
     * Get the key of the current record.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the key region of the current record, or NULL on failure.
     * @note If the cursor is invalidated, NULL is returned.  Because an additional zero
     * code is appended at the end of the region of the return value, the return value can be
     * treated as a C-style string.  Because the region of the return value is allocated with the
     * the new[] operator, it should be released with the delete[] operator when it is no longer
     * in use.
     */
    char* get_key(size_t* sp, bool step = false) {
      _assert_(sp);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_get_key", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      size_t ksiz;
      const char* kbuf = strmapget(outmap, "key", &ksiz);
      if (!kbuf) {
        db_->set_error(RPCClient::RVELOGIC, "no information");
        return NULL;
      }
      char* rbuf = new char[ksiz+1];
      std::memcpy(rbuf, kbuf, ksiz);
      rbuf[ksiz] = '\0';
      *sp = ksiz;
      return rbuf;
    }
    /**
     * Get the key of the current record.
     * @note Equal to the original Cursor::get_key method except that the parameter and the
     * return value are std::string.  The return value should be deleted explicitly by the
     * caller.
     */
    std::string* get_key(bool step = false) {
      _assert_(true);
      size_t ksiz;
      char* kbuf = get_key(&ksiz, step);
      if (!kbuf) return NULL;
      std::string* key = new std::string(kbuf, ksiz);
      delete[] kbuf;
      return key;
    }
    /**
     * Get the value of the current record.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the value region of the current record, or NULL on failure.
     * @note If the cursor is invalidated, NULL is returned.  Because an additional zero
     * code is appended at the end of the region of the return value, the return value can be
     * treated as a C-style string.  Because the region of the return value is allocated with the
     * the new[] operator, it should be released with the delete[] operator when it is no longer
     * in use.
     */
    char* get_value(size_t* sp, bool step = false) {
      _assert_(sp);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_get_value", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      size_t vsiz;
      const char* vbuf = strmapget(outmap, "value", &vsiz);
      if (!vbuf) {
        db_->set_error(RPCClient::RVELOGIC, "no information");
        return NULL;
      }
      char* rbuf = new char[vsiz+1];
      std::memcpy(rbuf, vbuf, vsiz);
      rbuf[vsiz] = '\0';
      *sp = vsiz;
      return rbuf;
    }
    /**
     * Get the value of the current record.
     * @note Equal to the original Cursor::get_value method except that the parameter and the
     * return value are std::string.  The return value should be deleted explicitly by the
     * caller.
     */
    std::string* get_value(bool step = false) {
      _assert_(true);
      size_t vsiz;
      char* vbuf = get_value(&vsiz, step);
      if (!vbuf) return NULL;
      std::string* value = new std::string(vbuf, vsiz);
      delete[] vbuf;
      return value;
    }
    /**
     * Get a pair of the key and the value of the current record.
     * @param ksp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param vbp the pointer to the variable into which the pointer to the value region is
     * assigned.
     * @param vsp the pointer to the variable into which the size of the value region is
     * assigned.
     * @param xtp the pointer to the variable into which the expiration time from now in seconds
     * is assigned.  If it is NULL, it is ignored.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the pair of the key region, or NULL on failure.
     * @note If the cursor is invalidated, NULL is returned.  Because an additional zero code is
     * appended at the end of each region of the key and the value, each region can be treated
     * as a C-style string.  The return value should be deleted explicitly by the caller with
     * the detele[] operator.
     */
    char* get(size_t* ksp, const char** vbp, size_t* vsp, int64_t* xtp = NULL,
              bool step = false) {
      _assert_(ksp && vbp && vsp);
      std::map<std::string, std::string> inmap;
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_get", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      size_t ksiz = 0;
      const char* kbuf = strmapget(outmap, "key", &ksiz);
      size_t vsiz = 0;
      const char* vbuf = strmapget(outmap, "value", &vsiz);
      if (!kbuf || !vbuf) {
        db_->set_error(RPCClient::RVELOGIC, "no information");
        return NULL;
      }
      const char* rp = strmapget(outmap, "xt");
      int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
      char* rbuf = new char[ksiz+vsiz+2];
      std::memcpy(rbuf, kbuf, ksiz);
      rbuf[ksiz] = '\0';
      std::memcpy(rbuf + ksiz + 1 , vbuf, vsiz);
      rbuf[ksiz+1+vsiz] = '\0';
      *ksp = ksiz;
      *vbp = rbuf + ksiz + 1;
      *vsp = vsiz;
      if (xtp) *xtp = xt;
      return rbuf;
    }
    /**
     * Get a pair of the key and the value of the current record.
     * @param xtp the pointer to the variable into which the expiration time from now in seconds
     * is assigned.  If it is NULL, it is ignored.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the pair of the key and the value, or NULL on failure.
     * @note Equal to the original Cursor::get method except that the return value is std::pair.
     * If the cursor is invalidated, NULL is returned.  The return value should be deleted
     * explicitly by the caller.
     */
    std::pair<std::string, std::string>* get_pair(int64_t* xtp = NULL, bool step = false) {
      _assert_(true);
      size_t ksiz, vsiz;
      const char* vbuf;
      int64_t xt = 0;
      char* kbuf = get(&ksiz, &vbuf, &vsiz, &xt, step);
      if (!kbuf) return NULL;
      typedef std::pair<std::string, std::string> Record;
      Record* rec = new Record(std::string(kbuf, ksiz), std::string(vbuf, vsiz));
      delete[] kbuf;
      if (xtp) *xtp = xt;
      return rec;
    }
    /**
     * Get the database object.
     * @return the database object.
     */
    RemoteDB* db() {
      _assert_(true);
      return db_;
    }
    /**
     * Get the last happened error.
     * @return the last happened error.
     */
    Error error() {
      _assert_(true);
      return db()->error();
    }
  private:
    /**
     * Set the parameter of the target cursor.
     * @param inmap the string map to contain the output parameters.
     */
    void set_cur_param(std::map<std::string, std::string>& inmap) {
      _assert_(true);
      kc::strprintf(&inmap["CUR"], "%lld", (long long)id_);
    }
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /** The inner database. */
    RemoteDB* db_;
    /** The ID number. */
    int64_t id_;
  };
  /**
   * Error data.
   */
  class Error {
  public:
    /**
     * Error codes.
     */
    enum Code {
      SUCCESS = RPCClient::RVSUCCESS,    ///< success
      INVALID = RPCClient::RVEINVALID,   ///< invalid operation
      LOGIC = RPCClient::RVELOGIC,       ///< logical inconsistency
      INTERNAL = RPCClient::RVEINTERNAL, ///< internal error
      NETWORK = RPCClient::RVENETWORK,   ///< network error
      EMISC = RPCClient::RVEMISC         ///< miscellaneous error
    };
    /**
     * Default constructor.
     */
    explicit Error() : code_(SUCCESS), message_("no error") {
      _assert_(true);
    }
    /**
     * Copy constructor.
     * @param src the source object.
     */
    Error(const Error& src) : code_(src.code_), message_(src.message_) {
      _assert_(true);
    }
    /**
     * Constructor.
     * @param code an error code.
     * @param message a supplement message.
     */
    explicit Error(Code code, const std::string& message) : code_(code), message_(message) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    ~Error() {
      _assert_(true);
    }
    /**
     * Set the error information.
     * @param code an error code.
     * @param message a supplement message.
     */
    void set(Code code, const std::string& message) {
      _assert_(true);
      code_ = code;
      message_ = message;
    }
    /**
     * Get the error code.
     * @return the error code.
     */
    Code code() const {
      _assert_(true);
      return code_;
    }
    /**
     * Get the readable string of the code.
     * @return the readable string of the code.
     */
    const char* name() const {
      _assert_(true);
      return codename(code_);
    }
    /**
     * Get the supplement message.
     * @return the supplement message.
     */
    const char* message() const {
      _assert_(true);
      return message_.c_str();
    }
    /**
     * Get the readable string of an error code.
     * @param code the error code.
     * @return the readable string of the error code.
     */
    static const char* codename(Code code) {
      _assert_(true);
      switch (code) {
        case SUCCESS: return "success";
        case INVALID: return "invalid operation";
        case LOGIC: return "logical inconsistency";
        case INTERNAL: return "internal error";
        case NETWORK: return "network error";
        default: break;
      }
      return "miscellaneous error";
    }
    /**
     * Assignment operator from the self type.
     * @param right the right operand.
     * @return the reference to itself.
     */
    Error& operator =(const Error& right) {
      _assert_(true);
      if (&right == this) return *this;
      code_ = right.code_;
      message_ = right.message_;
      return *this;
    }
    /**
     * Cast operator to integer.
     * @return the error code.
     */
    operator int32_t() const {
      return code_;
    }
  private:
    /** The error code. */
    Code code_;
    /** The supplement message. */
    std::string message_;
  };
  /**
   * Default constructor.
   */
  explicit RemoteDB() :
    rpc_(), ecode_(RPCClient::RVSUCCESS), emsg_("no error"), dbexpr_(""), curs_(), curcnt_(0) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  virtual ~RemoteDB() {
    _assert_(true);
    if (!curs_.empty()) {
      CursorList::const_iterator cit = curs_.begin();
      CursorList::const_iterator citend = curs_.end();
      while (cit != citend) {
        Cursor* cur = *cit;
        cur->db_ = NULL;
        cit++;
      }
    }
  }
  /**
   * Get the last happened error code.
   * @return the last happened error code.
   */
  Error error() const {
    _assert_(true);
    return Error((Error::Code)ecode_, emsg_);
  }
  /**
   * Open the connection.
   * @param host the name or the address of the server.  If it is an empty string, the local host
   * is specified.
   * @param port the port numger of the server.
   * @param timeout the timeout of each operation in seconds.  If it is not more than 0, no
   * timeout is specified.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& host = "", int32_t port = DEFPORT, double timeout = -1) {
    _assert_(true);
    if (!rpc_.open(host, port, timeout)) {
      set_error(RPCClient::RVENETWORK, "connection failed");
      return false;
    }
    return true;
  }
  /**
   * Close the connection.
   * @param grace true for graceful shutdown, or false for immediate disconnection.
   * @return true on success, or false on failure.
   */
  bool close(bool grace = true) {
    _assert_(true);
    return rpc_.close();
  }
  /**
   * Get the report of the server information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool report(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    strmap->clear();
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("report", NULL, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    strmap->insert(outmap.begin(), outmap.end());
    return true;
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool status(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    strmap->clear();
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("status", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    strmap->insert(outmap.begin(), outmap.end());
    return true;
  }
  /**
   * Remove all records.
   * @return true on success, or false on failure.
   */
  bool clear() {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("clear", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  int64_t count() {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("status", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    const char* rp = strmapget(outmap, "count");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes, or -1 on failure.
   */
  int64_t size() {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("status", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    const char* rp = strmapget(outmap, "size");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Set the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, a new record is created.  If the corresponding
   * record exists, the value is overwritten.
   */
  bool set(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
           int64_t xt = INT64_MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("set", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Set the value of a record.
   * @note Equal to the original DB::set method except that the parameters are std::string.
   */
  bool set(const std::string& key, const std::string& value, int64_t xt = INT64_MAX) {
    _assert_(true);
    return set(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Add a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, a new record is created.  If the corresponding
   * record exists, the record is not modified and false is returned.
   */
  bool add(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
           int64_t xt = INT64_MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("add", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Set the value of a record.
   * @note Equal to the original DB::add method except that the parameters are std::string.
   */
  bool add(const std::string& key, const std::string& value, int64_t xt = INT64_MAX) {
    _assert_(true);
    return add(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Replace the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, no new record is created and false is returned.
   * If the corresponding record exists, the value is modified.
   */
  bool replace(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
               int64_t xt = INT64_MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("replace", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Replace the value of a record.
   * @note Equal to the original DB::replace method except that the parameters are std::string.
   */
  bool replace(const std::string& key, const std::string& value, int64_t xt = INT64_MAX) {
    _assert_(true);
    return replace(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Append the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, a new record is created.  If the corresponding
   * record exists, the given value is appended at the end of the existing value.
   */
  bool append(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
              int64_t xt = INT64_MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("append", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Set the value of a record.
   * @note Equal to the original DB::append method except that the parameters are std::string.
   */
  bool append(const std::string& key, const std::string& value, int64_t xt = INT64_MAX) {
    _assert_(true);
    return append(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Add a number to the numeric integer value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param num the additional number.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return the result value, or INT64_MIN on failure.
   */
  int64_t increment(const char* kbuf, size_t ksiz, int64_t num, int64_t xt = INT64_MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    kc::strprintf(&inmap["num"], "%lld", (long long)num);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("increment", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return INT64_MIN;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return INT64_MIN;
    }
    return kc::atoi(rp);
  }
  /**
   * Add a number to the numeric integer value of a record.
   * @note Equal to the original DB::increment method except that the parameter is std::string.
   */
  int64_t increment(const std::string& key, int64_t num, int64_t xt = INT64_MAX) {
    _assert_(true);
    return increment(key.c_str(), key.size(), num, xt);
  }
  /**
   * Add a number to the numeric double value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param num the additional number.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return the result value, or Not-a-number on failure.
   */
  double increment_double(const char* kbuf, size_t ksiz, double num, int64_t xt = INT64_MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    kc::strprintf(&inmap["num"], "%f", num);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("increment_double", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return kc::nan();
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return kc::nan();
    }
    return kc::atof(rp);
  }
  /**
   * Add a number to the numeric double value of a record.
   * @note Equal to the original DB::increment_double method except that the parameter is
   * std::string.
   */
  double increment_double(const std::string& key, double num, int64_t xt = INT64_MAX) {
    _assert_(true);
    return increment_double(key.c_str(), key.size(), num, xt);
  }
  /**
   * Perform compare-and-swap.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param ovbuf the pointer to the old value region.  NULL means that no record corresponds.
   * @param ovsiz the size of the old value region.
   * @param nvbuf the pointer to the new value region.  NULL means that the record is removed.
   * @param nvsiz the size of new old value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   */
  bool cas(const char* kbuf, size_t ksiz,
           const char* ovbuf, size_t ovsiz, const char* nvbuf, size_t nvsiz,
           int64_t xt = INT64_MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    if (ovbuf) inmap["oval"] = std::string(ovbuf, ovsiz);
    if (nvbuf) inmap["nval"] = std::string(nvbuf, nvsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("cas", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Perform compare-and-swap.
   * @note Equal to the original DB::cas method except that the parameters are std::string.
   */
  bool cas(const std::string& key,
           const std::string& ovalue, const std::string& nvalue, int64_t xt = INT64_MAX) {
    _assert_(true);
    return cas(key.c_str(), key.size(),
               ovalue.c_str(), ovalue.size(), nvalue.c_str(), nvalue.size(), xt);
  }
  /**
   * Remove a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, false is returned.
   */
  bool remove(const char* kbuf, size_t ksiz) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("remove", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Remove a record.
   * @note Equal to the original DB::remove method except that the parameter is std::string.
   */
  bool remove(const std::string& key) {
    _assert_(true);
    return remove(key.c_str(), key.size());
  }
  /**
   * Retrieve the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @param xtp the pointer to the variable into which the expiration time from now in seconds
   * is assigned.  If it is NULL, it is ignored.
   * @return the pointer to the value region of the corresponding record, or NULL on failure.
   * @note If no record corresponds to the key, NULL is returned.  Because an additional zero
   * code is appended at the end of the region of the return value, the return value can be
   * treated as a C-style string.  Because the region of the return value is allocated with the
   * the new[] operator, it should be released with the delete[] operator when it is no longer
   * in use.
   */
  char* get(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp = NULL) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && sp);
    std::map<std::string, std::string> inmap;
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("get", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return NULL;
    }
    size_t vsiz;
    const char* vbuf = strmapget(outmap, "value", &vsiz);
    if (!vbuf) {
      set_error(RPCClient::RVELOGIC, "no information");
      return NULL;
    }
    const char* rp = strmapget(outmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : INT64_MAX;
    char* rbuf = new char[vsiz+1];
    std::memcpy(rbuf, vbuf, vsiz);
    rbuf[vsiz] = '\0';
    *sp = vsiz;
    if (xtp) *xtp = xt;
    return rbuf;
  }
  /**
   * Retrieve the value of a record.
   * @note Equal to the original DB::get method except that the parameter and the return value
   * are std::string.  The return value should be deleted explicitly by the caller.
   */
  std::string* get(const std::string& key, int64_t* xtp = NULL) {
    _assert_(true);
    size_t vsiz = 0;
    char* vbuf = get(key.c_str(), key.size(), &vsiz, xtp);
    if (!vbuf) return NULL;
    std::string* value = new std::string(vbuf, vsiz);
    delete[] vbuf;
    return value;
  }
  /**
   * Set the target database.
   * @param expr the expression of the target database.
   */
  void set_target(const std::string& expr) {
    _assert_(true);
    dbexpr_ = expr;
  }
  /**
   * Get the expression of the socket.
   * @return the expression of the socket or an empty string on failure.
   */
  const std::string expression() {
    _assert_(true);
    return rpc_.expression();
  }
  /**
   * Create a cursor object.
   * @return the return value is the created cursor object.
   * @note Because the object of the return value is allocated by the constructor, it should be
   * released with the delete operator when it is no longer in use.
   */
  Cursor* cursor() {
    _assert_(true);
    return new Cursor(this);
  }
private:
  /**
   * Set the parameter of the target database.
   * @param inmap the string map to contain the output parameters.
   */
  void set_db_param(std::map<std::string, std::string>& inmap) {
    _assert_(true);
    if (dbexpr_.empty()) return;
    inmap["DB"] = dbexpr_;
  }
  /**
   * Set the error status of RPC.
   * @param rv the return value by the RPC client.
   * @param message a supplement message.
   */
  void set_error(RPCClient::ReturnValue rv, const char* message) {
    _assert_(true);
    ecode_ = rv;
    emsg_ = message;
  }
  /**
   * Set the error status of RPC.
   * @param rv the return value by the RPC client.
   * @param outmap the string map to contain the output parameters.
   */
  void set_rpc_error(RPCClient::ReturnValue rv,
                     const std::map<std::string, std::string>& outmap) {
    _assert_(true);
    ecode_ = rv;
    size_t vsiz;
    const char* vbuf = strmapget(outmap, "ERROR", &vsiz);
    if (vbuf) {
      emsg_ = std::string(vbuf, vsiz);
    } else {
      emsg_ = "unexpected error";
    }
  }
  /** The RPC client. */
  RPCClient rpc_;
  /** The last happened error code. */
  RPCClient::ReturnValue ecode_;
  /** The last happened error messagee. */
  std::string emsg_;
  /** The target database expression. */
  std::string dbexpr_;
  /** The cursor objects. */
  CursorList curs_;
  /** The count of cursor generation. */
  int64_t curcnt_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
