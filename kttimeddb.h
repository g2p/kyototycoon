/*************************************************************************************************
 * Timed database
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


#ifndef _KTTIMEDDB_H                     // duplication check
#define _KTTIMEDDB_H

#include <ktcommon.h>

namespace kyototycoon {                  // common namespace


/**
 * Constants for implementation.
 */
namespace {
const uint8_t JDBMAGICDATA = 0xbb;       ///< magic data of the database type
const int64_t JDBXTSCUNIT = 256;         ///< score unit of expiratoin
const int64_t JDBXTREADFREQ = 8;         ///< inverse frequency of reading expiration
const int64_t JDBXTITERFREQ = 4;         ///< inverse frequency of iterating expiration
const int64_t JDBXTUNIT = 8;             ///< unit step number of expiration
}


/**
 * Timed database.
 * @note This class is a concrete class of a wrapper for the polymorphic database to add
 * expiration features.  This class can be inherited but overwriting methods is forbidden.
 * Before every database operation, it is necessary to call the TimedDB::open method in order to
 * open a database file and connect the database object to it.  To avoid data missing or
 * corruption, it is important to close every database file by the TimedDB::close method when
 * the database is no longer in use.  It is forbidden for multible database objects in a process
 * to open the same database at the same time.
 */
class TimedDB {
public:
  class Cursor;
  class Visitor;
private:
  class TimedVisitor;
  struct MergeLine;
public:
  /** The width of expiration time. */
  static const int32_t XTWIDTH = 5;
  /** The maximum number of expiration time. */
  static const int64_t XTMAX = (1LL << (XTWIDTH * 8)) - 1;
  /**
   * Cursor to indicate a record.
   */
  class Cursor {
    friend class TimedDB;
  public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(TimedDB* db) : db_(db), cur_(NULL), back_(false) {
      _assert_(db);
      cur_ = db_->db_.cursor();
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      _assert_(true);
      delete cur_;
    }
    /**
     * Jump the cursor to the first record for forward scan.
     * @return true on success, or false on failure.
     */
    bool jump() {
      _assert_(true);
      if (!cur_->jump()) return false;
      back_ = false;
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
      if (!cur_->jump(kbuf, ksiz)) return false;
      back_ = false;
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
      if (!cur_->jump_back()) return false;
      back_ = true;
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
      if (!cur_->jump_back(kbuf, ksiz)) return false;
      back_ = true;
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
      if (!cur_->step()) return false;
      back_ = false;
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
      if (!cur_->step_back()) return false;
      back_ = true;
      return true;
    }
    /**
     * Accept a visitor to the current record.
     * @param visitor a visitor object.
     * @param writable true for writable operation, or false for read-only operation.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return true on success, or false on failure.
     * @note the operation for each record is performed atomically and other threads accessing
     * the same record are blocked.
     */
    bool accept(Visitor* visitor, bool writable = true, bool step = false) {
      _assert_(visitor);
      bool err = false;
      int64_t ct = std::time(NULL);
      while (true) {
        TimedVisitor myvisitor(db_, visitor, ct, true);
        if (!cur_->accept(&myvisitor, writable, step)) {
          err = true;
          break;
        }
        if (myvisitor.again()) {
          if (!step && !(back_ ? cur_->step_back() : cur_->step())) {
            err = true;
            break;
          }
          continue;
        }
        break;
      }
      if (db_->xcur_) {
        int64_t xtsc = writable ? JDBXTSCUNIT : JDBXTSCUNIT / JDBXTREADFREQ;
        if (!db_->expire_records(xtsc)) err = true;
      }
      return !err;
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
      class VisitorImpl : public Visitor {
      public:
        explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
          vbuf_(vbuf), vsiz_(vsiz), xt_(xt), ok_(false) {}
        bool ok() const {
          return ok_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          ok_ = true;
          *sp = vsiz_;
          *xtp = xt_;
          return vbuf_;
        }
        const char* vbuf_;
        size_t vsiz_;
        int64_t xt_;
        bool ok_;
      };
      VisitorImpl visitor(vbuf, vsiz, xt);
      if (!accept(&visitor, true, step)) return false;
      if (!visitor.ok()) return false;
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
      class VisitorImpl : public Visitor {
      public:
        explicit VisitorImpl() : ok_(false) {}
        bool ok() const {
          return ok_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          ok_ = true;
          return REMOVE;
        }
        bool ok_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, true, false)) return false;
      if (!visitor.ok()) return false;
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
      class VisitorImpl : public Visitor {
      public:
        explicit VisitorImpl() : kbuf_(NULL), ksiz_(0) {}
        char* pop(size_t* sp) {
          *sp = ksiz_;
          return kbuf_;
        }
        void clear() {
          delete[] kbuf_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          kbuf_ = new char[ksiz+1];
          std::memcpy(kbuf_, kbuf, ksiz);
          kbuf_[ksiz] = '\0';
          ksiz_ = ksiz;
          return NOP;
        }
        char* kbuf_;
        size_t ksiz_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, false, step)) {
        visitor.clear();
        *sp = 0;
        return NULL;
      }
      size_t ksiz;
      char* kbuf = visitor.pop(&ksiz);
      if (!kbuf) {
        *sp = 0;
        return NULL;
      }
      *sp = ksiz;
      return kbuf;
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
      class VisitorImpl : public Visitor {
      public:
        explicit VisitorImpl() : vbuf_(NULL), vsiz_(0) {}
        char* pop(size_t* sp) {
          *sp = vsiz_;
          return vbuf_;
        }
        void clear() {
          delete[] vbuf_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          vbuf_ = new char[vsiz+1];
          std::memcpy(vbuf_, vbuf, vsiz);
          vbuf_[vsiz] = '\0';
          vsiz_ = vsiz;
          return NOP;
        }
        char* vbuf_;
        size_t vsiz_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, false, step)) {
        visitor.clear();
        *sp = 0;
        return NULL;
      }
      size_t vsiz;
      char* vbuf = visitor.pop(&vsiz);
      if (!vbuf) {
        *sp = 0;
        return NULL;
      }
      *sp = vsiz;
      return vbuf;
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
     * @param xtp the pointer to the variable into which the absolute expiration time is
     * assigned.  If it is NULL, it is ignored.
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
      class VisitorImpl : public Visitor {
      public:
        explicit VisitorImpl() : kbuf_(NULL), ksiz_(0), vbuf_(NULL), vsiz_(0), xt_(0) {}
        char* pop(size_t* ksp, const char** vbp, size_t* vsp, int64_t* xtp) {
          *ksp = ksiz_;
          *vbp = vbuf_;
          *vsp = vsiz_;
          if (xtp) *xtp = xt_;
          return kbuf_;
        }
        void clear() {
          delete[] kbuf_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          size_t rsiz = ksiz + 1 + vsiz + 1;
          kbuf_ = new char[rsiz];
          std::memcpy(kbuf_, kbuf, ksiz);
          kbuf_[ksiz] = '\0';
          ksiz_ = ksiz;
          vbuf_ = kbuf_ + ksiz + 1;
          std::memcpy(vbuf_, vbuf, vsiz);
          vbuf_[vsiz] = '\0';
          vsiz_ = vsiz;
          xt_ = *xtp;
          return NOP;
        }
        char* kbuf_;
        size_t ksiz_;
        char* vbuf_;
        size_t vsiz_;
        int64_t xt_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, false, step)) {
        visitor.clear();
        *ksp = 0;
        *vbp = NULL;
        *vsp = 0;
        if (xtp) *xtp = 0;
        return NULL;
      }
      return visitor.pop(ksp, vbp, vsp, xtp);
    }
    /**
     * Get a pair of the key and the value of the current record.
     * @param xtp the pointer to the variable into which the absolute expiration time is
     * assigned.  If it is NULL, it is ignored.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the pair of the key and the value, or NULL on failure.
     * @note Equal to the original Cursor::get method except that the return value is std::pair.
     * If the cursor is invalidated, NULL is returned.  The return value should be deleted
     * explicitly by the caller.
     */
    std::pair<std::string, std::string>* get_pair(int64_t* xtp = NULL, bool step = false) {
      _assert_(true);
      typedef std::pair<std::string, std::string> Record;
      class VisitorImpl : public Visitor {
      public:
        explicit VisitorImpl() : rec_(NULL) {}
        Record* pop(int64_t* xtp) {
          if (xtp) *xtp = xt_;
          return rec_;
        }
      private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          std::string key(kbuf, ksiz);
          std::string value(vbuf, vsiz);
          rec_ = new Record(key, value);
          xt_ = *xtp;
          return NOP;
        }
        Record* rec_;
        int64_t xt_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, false, step)) return NULL;
      return visitor.pop(xtp);
    }
    /**
     * Get the database object.
     * @return the database object.
     */
    TimedDB* db() {
      _assert_(true);
      return db_;
    }
    /**
     * Get the last happened error.
     * @return the last happened error.
     */
    kc::BasicDB::Error error() {
      _assert_(true);
      return db()->error();
    }
  private:
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /** The inner database. */
    TimedDB* db_;
    /** The inner cursor. */
    kc::BasicDB::Cursor* cur_;
    /** The backward flag. */
    bool back_;
  };
  /**
   * Interface to access a record.
   */
  class Visitor {
  public:
    /** Special pointer for no operation. */
    static const char* const NOP;
    /** Special pointer to remove the record. */
    static const char* const REMOVE;
    /**
     * Destructor.
     */
    virtual ~Visitor() {
      _assert_(true);
    }
    /**
     * Visit a record.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @param vbuf the pointer to the value region.
     * @param vsiz the size of the value region.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param xtp the pointer to the variable into which the expiration time from now in seconds
     * is assigned.  The initial value is the absolute expiration time.
     * @return If it is the pointer to a region, the value is replaced by the content.  If it
     * is Visitor::NOP, nothing is modified.  If it is Visitor::REMOVE, the record is removed.
     */
    virtual const char* visit_full(const char* kbuf, size_t ksiz,
                                   const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ && sp && xtp);
      return NOP;
    }
    /**
     * Visit a empty record space.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param xtp the pointer to the variable into which the expiration time from now in seconds
     * is assigned.
     * @return If it is the pointer to a region, the value is replaced by the content.  If it
     * is Visitor::NOP or Visitor::REMOVE, nothing is modified.
     */
    virtual const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && sp && xtp);
      return NOP;
    }
  };
  /**
   * Merge modes.
   */
  enum MergeMode {
    MSET,                                ///< overwrite the existing value
    MADD,                                ///< keep the existing value
    MREPLACE,                            ///< modify the existing record only
    MAPPEND                              ///< append the new value
  };
  /**
   * Default constructor.
   */
  explicit TimedDB() :
    xlock_(), db_(), omode_(0), opts_(0), capcnt_(0), capsiz_(0), xcur_(NULL), xsc_(0) {
    _assert_(true);
  }
  /**
   * Constructor.
   * @param db the internal database object.  Its possession is transferred inside and the
   * object is deleted automatically.
   */
  explicit TimedDB(kc::BasicDB* db) :
    xlock_(), db_(db), omode_(0), opts_(0), xcur_(NULL), xsc_(0) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  virtual ~TimedDB() {
    _assert_(true);
    if (omode_ != 0) close();
  }
  /**
   * Get the last happened error.
   * @return the last happened error.
   */
  kc::BasicDB::Error error() const {
    _assert_(true);
    return db_.error();
  }
  /**
   * Set the error information.
   * @param code an error code.
   * @param message a supplement message.
   */
  void set_error(kc::BasicDB::Error::Code code, const char* message) {
    _assert_(message);
    db_.set_error(code, message);
  }
  /**
   * Open a database file.
   * @param path the path of a database file.  The same as with kc::PolyDB.  In addition, the
   * following tuning parameters are supported.  "ktopts" sets options and the value can contain
   * "p" for the persistent option.  "ktcapcnt" sets the capacity by record number.  "ktcapsiz"
   * sets the capacity by database size.
   * @param mode the connection mode.  The same as with kc::PolyDB.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& path = ":",
            uint32_t mode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE) {
    _assert_(true);
    if (omode_ != 0) {
      set_error(kc::BasicDB::Error::INVALID, "already opened");
      return false;
    }
    kc::ScopedSpinLock lock(&xlock_);
    std::vector<std::string> elems;
    kc::strsplit(path, '#', &elems);
    capcnt_ = -1;
    capsiz_ = -1;
    opts_ = 0;
    std::vector<std::string>::iterator it = elems.begin();
    std::vector<std::string>::iterator itend = elems.end();
    if (it != itend) it++;
    while (it != itend) {
      std::vector<std::string> fields;
      if (kc::strsplit(*it, '=', &fields) > 1) {
        const char* key = fields[0].c_str();
        const char* value = fields[1].c_str();
        if (!std::strcmp(key, "ktcapcnt") || !std::strcmp(key, "ktcapcount") ||
            !std::strcmp(key, "ktcap_count")) {
          capcnt_ = kc::atoix(value);
        } else if (!std::strcmp(key, "ktcapsiz") || !std::strcmp(key, "ktcapsize") ||
                   !std::strcmp(key, "ktcap_size")) {
          capsiz_ = kc::atoix(value);
        } else if (!std::strcmp(key, "ktopts") || !std::strcmp(key, "ktoptions")) {
          if (std::strchr(value, 'p')) opts_ |= TPERSIST;
        }
      }
      it++;
    }
    if (!db_.open(path, mode)) return false;
    kc::BasicDB* idb = db_.reveal_inner_db();
    if (idb) {
      const std::type_info& info = typeid(*idb);
      if (info == typeid(kc::HashDB)) {
        kc::HashDB* hdb = (kc::HashDB*)idb;
        char* opq = hdb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == JDBMAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && hdb->count() < 1) {
            *(uint8_t*)opq = JDBMAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            hdb->synchronize_opaque();
          }
        }
      } else if (info == typeid(kc::TreeDB)) {
        kc::TreeDB* tdb = (kc::TreeDB*)idb;
        char* opq = tdb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == JDBMAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && tdb->count() < 1) {
            *(uint8_t*)opq = JDBMAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            tdb->synchronize_opaque();
          }
        }
      } else if (info == typeid(kc::DirDB)) {
        kc::DirDB* ddb = (kc::DirDB*)idb;
        char* opq = ddb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == JDBMAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && ddb->count() < 1) {
            *(uint8_t*)opq = JDBMAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            ddb->synchronize_opaque();
          }
        }
      } else if (info == typeid(kc::ForestDB)) {
        kc::ForestDB* fdb = (kc::ForestDB*)idb;
        char* opq = fdb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == JDBMAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && fdb->count() < 1) {
            *(uint8_t*)opq = JDBMAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            fdb->synchronize_opaque();
          }
        }
      }
    }
    omode_ = mode;
    if ((omode_ & kc::BasicDB::OWRITER) && !(opts_ & TPERSIST)) {
      xcur_ = db_.cursor();
      if (db_.count() > 0) xcur_->jump();
    }
    xsc_ = 0;
    return true;
  }
  /**
   * Close the database file.
   * @return true on success, or false on failure.
   */
  bool close() {
    _assert_(true);
    if (omode_ == 0) {
      set_error(kc::BasicDB::Error::INVALID, "not opened");
      return false;
    }
    kc::ScopedSpinLock lock(&xlock_);
    bool err = false;
    delete xcur_;
    xcur_ = NULL;
    if (!db_.close()) err = true;
    omode_ = 0;
    return !err;
  }
  /**
   * Accept a visitor to a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @return true on success, or false on failure.
   * @note the operation for each record is performed atomically and other threads accessing the
   * same record are blocked.
   */
  bool accept(const char* kbuf, size_t ksiz, Visitor* visitor, bool writable = true) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && visitor);
    bool err = false;
    int64_t ct = std::time(NULL);
    TimedVisitor myvisitor(this, visitor, ct, false);
    if (!db_.accept(kbuf, ksiz, &myvisitor, writable)) err = true;
    if (xcur_) {
      int64_t xtsc = writable ? JDBXTSCUNIT : JDBXTSCUNIT / JDBXTREADFREQ;
      if (!expire_records(xtsc)) err = true;
    }
    return !err;
  }
  /**
   * Iterate to accept a visitor for each record.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   * @note The whole iteration is performed atomically and other threads are blocked.
   */
  bool iterate(Visitor *visitor, bool writable = true,
               kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(visitor);
    bool err = false;
    int64_t ct = std::time(NULL);
    TimedVisitor myvisitor(this, visitor, ct, true);
    if (!db_.iterate(&myvisitor, writable, checker)) err = true;
    if (xcur_) {
      int64_t count = db_.count();
      int64_t xtsc = writable ? JDBXTSCUNIT : JDBXTSCUNIT / JDBXTREADFREQ;
      if (count > 0) xtsc *= count / JDBXTITERFREQ;
      if (!expire_records(xtsc)) err = true;
    }
    return !err;
  }
  /**
   * Synchronize updated contents with the file and the device.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @param proc a postprocessor object.  If it is NULL, no postprocessing is performed.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool synchronize(bool hard = false, kc::BasicDB::FileProcessor* proc = NULL,
                   kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.synchronize(hard, proc, checker);
  }
  /**
   * Create a copy of the database file.
   * @param dest the path of the destination file.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool copy(const std::string& dest, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.copy(dest, checker);
  }
  /**
   * Begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction(bool hard = false) {
    _assert_(true);
    return db_.begin_transaction(hard);
  }
  /**
   * Try to begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction_try(bool hard = false) {
    _assert_(true);
    return db_.begin_transaction_try(hard);
  }
  /**
   * End transaction.
   * @param commit true to commit the transaction, or false to abort the transaction.
   * @return true on success, or false on failure.
   */
  bool end_transaction(bool commit = true) {
    _assert_(true);
    return db_.end_transaction(commit);
  }
  /**
   * Remove all records.
   * @return true on success, or false on failure.
   */
  bool clear() {
    _assert_(true);
    return db_.clear();
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  int64_t count() {
    _assert_(true);
    return db_.count();
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes, or -1 on failure.
   */
  int64_t size() {
    _assert_(true);
    return db_.size();
  }
  /**
   * Get the path of the database file.
   * @return the path of the database file, or an empty string on failure.
   */
  std::string path() {
    _assert_(true);
    return db_.path();
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool status(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    if (!db_.status(strmap)) return false;
    (*strmap)["ktopts"] = kc::strprintf("%u", opts_);
    (*strmap)["ktcapcnt"] = kc::strprintf("%lld", (long long)capcnt_);
    (*strmap)["ktcapsiz"] = kc::strprintf("%lld", (long long)capsiz_);
    return true;
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
        vbuf_(vbuf), vsiz_(vsiz), xt_(xt) {}
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
        vbuf_(vbuf), vsiz_(vsiz), xt_(xt), ok_(false) {}
      bool ok() const {
        return ok_;
      }
    private:
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        ok_ = true;
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
      bool ok_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::DUPREC, "record duplication");
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
        vbuf_(vbuf), vsiz_(vsiz), xt_(xt), ok_(false) {}
      bool ok() const {
        return ok_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        ok_ = true;
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
      bool ok_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
        vbuf_(vbuf), vsiz_(vsiz), xt_(xt), nbuf_(NULL) {}
      ~VisitorImpl() {
        if (nbuf_) delete[] nbuf_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        size_t nsiz = vsiz + vsiz_;
        nbuf_ = new char[nsiz];
        std::memcpy(nbuf_, vbuf, vsiz);
        std::memcpy(nbuf_ + vsiz, vbuf_, vsiz_);
        *sp = nsiz;
        *xtp = xt_;
        return nbuf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
      char* nbuf_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(int64_t num, int64_t xt) : num_(num), xt_(xt), big_(0) {}
      int64_t num() {
        return num_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        if (vsiz != sizeof(num_)) {
          num_ = INT64_MIN;
          return NOP;
        }
        int64_t onum;
        std::memcpy(&onum, vbuf, vsiz);
        onum = kc::ntoh64(onum);
        if (num_ == 0) {
          num_ = onum;
          return NOP;
        }
        num_ += onum;
        big_ = kc::hton64(num_);
        *sp = sizeof(big_);
        *xtp = xt_;
        return (const char*)&big_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        big_ = kc::hton64(num_);
        *sp = sizeof(big_);
        *xtp = xt_;
        return (const char*)&big_;
      }
      int64_t num_;
      int64_t xt_;
      uint64_t big_;
    };
    VisitorImpl visitor(num, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return INT64_MIN;
    num = visitor.num();
    if (num == INT64_MIN) {
      set_error(kc::BasicDB::Error::LOGIC, "logical inconsistency");
      return num;
    }
    return num;
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(double num, int64_t xt) :
        DECUNIT(1000000000000000LL), num_(num), xt_(xt), buf_() {}
      double num() {
        return num_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        if (vsiz != sizeof(buf_)) {
          num_ = kc::nan();
          return NOP;
        }
        int64_t linteg, lfract;
        std::memcpy(&linteg, vbuf, sizeof(linteg));
        linteg = kc::ntoh64(linteg);
        std::memcpy(&lfract, vbuf + sizeof(linteg), sizeof(lfract));
        lfract = kc::ntoh64(lfract);
        if (lfract == INT64_MIN && linteg == INT64_MIN) {
          num_ = kc::nan();
          return NOP;
        } else if (linteg == INT64_MAX) {
          num_ = HUGE_VAL;
          return NOP;
        } else if (linteg == INT64_MIN) {
          num_ = -HUGE_VAL;
          return NOP;
        }
        if (num_ == 0.0) {
          num_ = linteg + (double)lfract / DECUNIT;
          return NOP;
        }
        long double dinteg;
        long double dfract = std::modfl(num_, &dinteg);
        if (kc::chknan(dinteg)) {
          linteg = INT64_MIN;
          lfract = INT64_MIN;
          num_ = kc::nan();
        } else if (kc::chkinf(dinteg)) {
          linteg = dinteg > 0 ? INT64_MAX : INT64_MIN;
          lfract = 0;
          num_ = dinteg;
        } else {
          linteg += (int64_t)dinteg;
          lfract += (int64_t)(dfract * DECUNIT);
          if (lfract >= DECUNIT) {
            linteg += 1;
            lfract -= DECUNIT;
          }
          num_ = linteg + (double)lfract / DECUNIT;
        }
        linteg = kc::hton64(linteg);
        std::memcpy(buf_, &linteg, sizeof(linteg));
        lfract = kc::hton64(lfract);
        std::memcpy(buf_ + sizeof(linteg), &lfract, sizeof(lfract));
        *sp = sizeof(buf_);
        *xtp = xt_;
        return buf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        long double dinteg;
        long double dfract = std::modfl(num_, &dinteg);
        int64_t linteg, lfract;
        if (kc::chknan(dinteg)) {
          linteg = INT64_MIN;
          lfract = INT64_MIN;
        } else if (kc::chkinf(dinteg)) {
          linteg = dinteg > 0 ? INT64_MAX : INT64_MIN;
          lfract = 0;
        } else {
          linteg = (int64_t)dinteg;
          lfract = (int64_t)(dfract * DECUNIT);
        }
        linteg = kc::hton64(linteg);
        std::memcpy(buf_, &linteg, sizeof(linteg));
        lfract = kc::hton64(lfract);
        std::memcpy(buf_ + sizeof(linteg), &lfract, sizeof(lfract));
        *sp = sizeof(buf_);
        *xtp = xt_;
        return buf_;
      }
      const int64_t DECUNIT;
      double num_;
      int64_t xt_;
      char buf_[sizeof(int64_t)*2];
    };
    VisitorImpl visitor(num, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return kc::nan();
    num = visitor.num();
    if (kc::chknan(num)) {
      set_error(kc::BasicDB::Error::LOGIC, "logical inconsistency");
      return kc::nan();
    }
    return num;
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(const char* ovbuf, size_t ovsiz, const char* nvbuf, size_t nvsiz,
                           int64_t xt) :
        ovbuf_(ovbuf), ovsiz_(ovsiz), nvbuf_(nvbuf), nvsiz_(nvsiz), xt_(xt), ok_(false) {}
      bool ok() const {
        return ok_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        if (!ovbuf_ || vsiz != ovsiz_ || std::memcmp(vbuf, ovbuf_, vsiz)) return NOP;
        ok_ = true;
        if (!nvbuf_) return REMOVE;
        *sp = nvsiz_;
        *xtp = xt_;
        return nvbuf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        if (ovbuf_) return NOP;
        ok_ = true;
        if (!nvbuf_) return NOP;
        *sp = nvsiz_;
        *xtp = xt_;
        return nvbuf_;
      }
      const char* ovbuf_;
      size_t ovsiz_;
      const char* nvbuf_;
      size_t nvsiz_;
      int64_t xt_;
      bool ok_;
    };
    VisitorImpl visitor(ovbuf, ovsiz, nvbuf, nvsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::LOGIC, "status conflict");
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
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl() : ok_(false) {}
      bool ok() const {
        return ok_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        ok_ = true;
        return REMOVE;
      }
      bool ok_;
    };
    VisitorImpl visitor;
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
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
   * @param xtp the pointer to the variable into which the absolute expiration time is assigned.
   * If it is NULL, it is ignored.
   * @return the pointer to the value region of the corresponding record, or NULL on failure.
   * @note If no record corresponds to the key, NULL is returned.  Because an additional zero
   * code is appended at the end of the region of the return value, the return value can be
   * treated as a C-style string.  Because the region of the return value is allocated with the
   * the new[] operator, it should be released with the delete[] operator when it is no longer
   * in use.
   */
  char* get(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp = NULL) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && sp);
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl() : vbuf_(NULL), vsiz_(0), xt_(0) {}
      char* pop(size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        if (xtp) *xtp = xt_;
        return vbuf_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        vbuf_ = new char[vsiz+1];
        std::memcpy(vbuf_, vbuf, vsiz);
        vbuf_[vsiz] = '\0';
        vsiz_ = vsiz;
        xt_ = *xtp;
        return NOP;
      }
      char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor;
    if (!accept(kbuf, ksiz, &visitor, false)) {
      *sp = 0;
      return NULL;
    }
    size_t vsiz;
    char* vbuf = visitor.pop(&vsiz, xtp);
    if (!vbuf) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      *sp = 0;
      return NULL;
    }
    *sp = vsiz;
    return vbuf;
  }
  /**
   * Retrieve the value of a record.
   * @note Equal to the original DB::get method except that the parameter and the return value
   * are std::string.  The return value should be deleted explicitly by the caller.
   */
  std::string* get(const std::string& key, int64_t* xtp = NULL) {
    _assert_(true);
    size_t vsiz;
    char* vbuf = get(key.c_str(), key.size(), &vsiz, xtp);
    if (!vbuf) return NULL;
    std::string* value = new std::string(vbuf, vsiz);
    delete[] vbuf;
    return value;
  }
  /**
   * Retrieve the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the buffer into which the value of the corresponding record is
   * written.
   * @param max the size of the buffer.
   * @param xtp the pointer to the variable into which the expiration time from now in seconds
   * is assigned.  If it is NULL, it is ignored.
   * @return the size of the value, or -1 on failure.
   */
  int32_t get(const char* kbuf, size_t ksiz, char* vbuf, size_t max, int64_t* xtp = NULL) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf);
    class VisitorImpl : public Visitor {
    public:
      explicit VisitorImpl(char* vbuf, size_t max) :
        vbuf_(vbuf), max_(max), vsiz_(-1), xt_(0) {}
      int32_t vsiz(int64_t* xtp) {
        if (xtp) *xtp = xt_;
        return vsiz_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        vsiz_ = vsiz;
        xt_ = *xtp;
        size_t max = vsiz < max_ ? vsiz : max_;
        std::memcpy(vbuf_, vbuf, max);
        return NOP;
      }
      char* vbuf_;
      size_t max_;
      int32_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor(vbuf, max);
    if (!accept(kbuf, ksiz, &visitor, false)) return -1;
    int32_t vsiz = visitor.vsiz(xtp);
    if (vsiz < 0) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      return -1;
    }
    return vsiz;
  }
  /**
   * Dump records into a data stream.
   * @param dest the destination stream.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool dump_snapshot(std::ostream* dest, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(dest);
    return db_.dump_snapshot(dest, checker);
  }
  /**
   * Dump records into a file.
   * @param dest the path of the destination file.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool dump_snapshot(const std::string& dest, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.dump_snapshot(dest, checker);
  }
  /**
   * Load records from a data stream.
   * @param src the source stream.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool load_snapshot(std::istream* src, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(src);
    return db_.load_snapshot(src, checker);
  }
  /**
   * Load records from a file.
   * @param src the path of the source file.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool load_snapshot(const std::string& src, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.load_snapshot(src, checker);
  }
  /**
   * Reveal the inner database object.
   * @return the inner database object, or NULL on failure.
   */
  kc::BasicDB* reveal_inner_db() {
    _assert_(true);
    return db_.reveal_inner_db();
  }
  /**
   * Scan the database and eliminate regions of expired records.
   * @param step the number of steps.  If it is not more than 0, the whole region is scanned.
   * @return true on success, or false on failure.
   */
  bool vacuum(int64_t step = 0) {
    _assert_(true);
    bool err = false;
    if (xcur_) {
      if (step > 1) {
        if (step > INT64_MAX / JDBXTSCUNIT) step = INT64_MAX / JDBXTSCUNIT;
        if (!expire_records(step * JDBXTSCUNIT)) err = true;
      } else {
        xcur_->jump();
        xsc_ = 0;
        if (!expire_records(INT64_MAX)) err = true;
        xsc_ = 0;
      }
    }
    if (!defrag(step)) err = true;
    return !err;
  }
  /**
   * Get keys matching a prefix string.
   * @param prefix the prefix string.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_prefix(const std::string& prefix, std::vector<std::string>* strvec,
                       int64_t max = -1, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(strvec);
    return db_.match_prefix(prefix, strvec, max, checker);
  }
  /**
   * Get keys matching a regular expression string.
   * @param regex the regular expression string.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_regex(const std::string& regex, std::vector<std::string>* strvec,
                      int64_t max = -1, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(strvec);
    return db_.match_regex(regex, strvec, max, checker);
  }
  /**
   * Merge records from other databases.
   * @param srcary an array of the source detabase objects.
   * @param srcnum the number of the elements of the source array.
   * @param mode the merge mode.  TimedDB::MSET to overwrite the existing value, TimedDB::MADD to
   * keep the existing value, TimedDB::MREPLACE to modify the existing record only,
   * TimedDB::MAPPEND to append the new value.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool merge(TimedDB** srcary, size_t srcnum, MergeMode mode = MSET,
             kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(srcary && srcnum <= kc::MEMMAXSIZ);
    bool err = false;
    kc::Comparator* comp = &kc::LEXICALCOMP;
    std::priority_queue<MergeLine> lines;
    int64_t allcnt = 0;
    for (size_t i = 0; i < srcnum; i++) {
      MergeLine line;
      line.cur = srcary[i]->cursor();
      line.comp = comp;
      line.cur->jump();
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, &line.xt, true);
      if (line.kbuf) {
        lines.push(line);
        int64_t count = srcary[i]->count();
        if (count > 0) allcnt += count;
      } else {
        delete line.cur;
      }
    }
    if (checker && !checker->check("merge", "beginning", 0, allcnt)) {
      set_error(kc::BasicDB::Error::LOGIC, "checker failed");
      err = true;
    }
    int64_t curcnt = 0;
    while (!err && !lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      switch (mode) {
        case MSET: {
          if (!set(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt)) err = true;
          break;
        }
        case MADD: {
          if (!add(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt) &&
              error() != kc::BasicDB::Error::DUPREC) err = true;
          break;
        }
        case MREPLACE: {
          if (!replace(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt) &&
              error() != kc::BasicDB::Error::NOREC) err = true;
          break;
        }
        case MAPPEND: {
          if (!append(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt)) err = true;
          break;
        }
      }
      delete[] line.kbuf;
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, &line.xt, true);
      if (line.kbuf) {
        lines.push(line);
      } else {
        delete line.cur;
      }
      curcnt++;
      if (checker && !checker->check("merge", "processing", curcnt, allcnt)) {
        set_error(kc::BasicDB::Error::LOGIC, "checker failed");
        err = true;
        break;
      }
    }
    if (checker && !checker->check("merge", "ending", -1, allcnt)) {
      set_error(kc::BasicDB::Error::LOGIC, "checker failed");
      err = true;
    }
    while (!lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      delete[] line.kbuf;
      delete line.cur;
    }
    return !err;
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
  /**
   * Set the internal logger.
   * @param logger the logger object.  The same as with kc::BasicDB.
   * @param kinds kinds of logged messages by bitwise-or:  The same as with kc::BasicDB.
   * @return true on success, or false on failure.
   */
  bool tune_logger(kc::BasicDB::Logger* logger,
                   uint32_t kinds = kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR) {
    _assert_(logger);
    return db_.tune_logger(logger, kinds);
  }
private:
  /**
   * Tuning Options.
   */
  enum Option {
    TPERSIST = 1 << 1                    ///< disable expiration
  };
  /**
   * Visitor to handle records with time stamps.
   */
  class TimedVisitor : public kc::BasicDB::Visitor {
  public:
    TimedVisitor(TimedDB* db, TimedDB::Visitor* visitor, int64_t ct, bool isiter) :
      db_(db), visitor_(visitor), ct_(ct), isiter_(isiter), jbuf_(NULL), again_(false) {
      _assert_(db && visitor && ct >= 0);
    }
    ~TimedVisitor() {
      _assert_(true);
      delete[] jbuf_;
    }
    bool again() {
      _assert_(true);
      return again_;
    }
  private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      _assert_(kbuf && vbuf && sp);
      if (db_->opts_ & TimedDB::TPERSIST) {
        int64_t xt = INT64_MAX;
        return visitor_->visit_full(kbuf, ksiz, vbuf, vsiz, sp, &xt);
      }
      if (vsiz < (size_t)XTWIDTH) return NOP;
      int64_t xt = kc::readfixnum(vbuf, XTWIDTH);
      if (ct_ > xt) {
        if (isiter_) {
          again_ = true;
          return NOP;
        }
        db_->set_error(kc::BasicDB::Error::NOREC, "no record (expired)");
        size_t rsiz;
        const char* rbuf = visitor_->visit_empty(kbuf, ksiz, &rsiz, &xt);
        if (rbuf == TimedDB::Visitor::NOP) return NOP;
        if (rbuf == TimedDB::Visitor::REMOVE) return REMOVE;
        xt = modify_exptime(xt, ct_);
        jbuf_ = make_record_value(rbuf, rsiz, xt, sp);
        return jbuf_;
      }
      vbuf += XTWIDTH;
      vsiz -= XTWIDTH;
      size_t rsiz;
      const char* rbuf = visitor_->visit_full(kbuf, ksiz, vbuf, vsiz, &rsiz, &xt);
      if (rbuf == TimedDB::Visitor::NOP) return NOP;
      if (rbuf == TimedDB::Visitor::REMOVE) return REMOVE;
      xt = modify_exptime(xt, ct_);
      jbuf_ = make_record_value(rbuf, rsiz, xt, sp);
      return jbuf_;
    }
    const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp) {
      if (db_->opts_ & TimedDB::TPERSIST) {
        int64_t xt = INT64_MAX;
        return visitor_->visit_empty(kbuf, ksiz, sp, &xt);
      }
      size_t rsiz;
      int64_t xt = -1;
      const char* rbuf = visitor_->visit_empty(kbuf, ksiz, &rsiz, &xt);
      if (rbuf == TimedDB::Visitor::NOP) return NOP;
      if (rbuf == TimedDB::Visitor::REMOVE) return REMOVE;
      xt = modify_exptime(xt, ct_);
      jbuf_ = make_record_value(rbuf, rsiz, xt, sp);
      return jbuf_;
    }
    TimedDB* db_;
    TimedDB::Visitor* visitor_;
    int64_t ct_;
    bool isiter_;
    char* jbuf_;
    bool again_;
  };
  /**
   * Front line of a merging list.
   */
  struct MergeLine {
    TimedDB::Cursor* cur;                ///< cursor
    kc::Comparator* comp;                ///< comparator
    char* kbuf;                          ///< pointer to the key
    size_t ksiz;                         ///< size of the key
    const char* vbuf;                    ///< pointer to the value
    size_t vsiz;                         ///< size of the value
    int64_t xt;                          ///< expiration time
    /** comparing operator */
    bool operator <(const MergeLine& right) const {
      return comp->compare(kbuf, ksiz, right.kbuf, right.ksiz) > 0;
    }
  };
  /**
   * Remove expired records.
   * @param score the score of expiration.
   * @return true on success, or false on failure.
   */
  bool expire_records(int64_t score) {
    _assert_(score >= 0);
    xsc_ += score;
    if (xsc_ < JDBXTSCUNIT * JDBXTUNIT) return true;
    if (!xlock_.lock_try()) return true;
    int64_t step = (int64_t)xsc_ / JDBXTSCUNIT;
    xsc_ -= step * JDBXTSCUNIT;
    int64_t ct = std::time(NULL);
    class VisitorImpl : public kc::BasicDB::Visitor {
    public:
      VisitorImpl(int64_t ct) : ct_(ct) {}
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp) {
        if (vsiz < (size_t)XTWIDTH) return NOP;
        int64_t xt = kc::readfixnum(vbuf, XTWIDTH);
        if (ct_ <= xt) return NOP;
        return REMOVE;
      }
      int64_t ct_;
    };
    VisitorImpl visitor(ct);
    bool err = false;
    for (int64_t i = 0; i < step; i++) {
      if (!xcur_->accept(&visitor, true, true)) {
        kc::BasicDB::Error::Code code = db_.error().code();
        if (code == kc::BasicDB::Error::INVALID || code == kc::BasicDB::Error::NOREC) {
          xcur_->jump();
        } else {
          err = true;
        }
        xsc_ = 0;
        break;
      }
    }
    if (capcnt_ > 0) {
      int64_t count = db_.count();
      while (count > capcnt_) {
        if (!xcur_->remove()) {
          kc::BasicDB::Error::Code code = db_.error().code();
          if (code == kc::BasicDB::Error::INVALID || code == kc::BasicDB::Error::NOREC) {
            xcur_->jump();
          } else {
            err = true;
          }
          break;
        }
        count--;
      }
      if (!defrag(step)) err = true;
    }
    if (capsiz_ > 0) {
      int64_t size = db_.size();
      if (size > capsiz_) {
        for (int64_t i = 0; i < step; i++) {
          if (!xcur_->remove()) {
            kc::BasicDB::Error::Code code = db_.error().code();
            if (code == kc::BasicDB::Error::INVALID || code == kc::BasicDB::Error::NOREC) {
              xcur_->jump();
            } else {
              err = true;
            }
            break;
          }
        }
        if (!defrag(step)) err = true;
      }
    }
    xlock_.unlock();
    return !err;
  }
  /**
   * Perform defragmentation of the database file.
   * @param step the number of steps.  If it is not more than 0, the whole region is defraged.
   * @return true on success, or false on failure.
   */
  bool defrag(int step) {
    _assert_(true);
    bool err = false;
    kc::BasicDB* idb = db_.reveal_inner_db();
    if (idb) {
      const std::type_info& info = typeid(*idb);
      if (info == typeid(kc::HashDB)) {
        kc::HashDB* hdb = (kc::HashDB*)idb;
        if (!hdb->defrag(step)) err = true;
      } else if (info == typeid(kc::TreeDB)) {
        kc::TreeDB* tdb = (kc::TreeDB*)idb;
        if (!tdb->defrag(step)) err = true;
      }
    }
    return !err;
  }
  /**
   * Make the record value with meta data.
   * @param vbuf the buffer of the original record value.
   * @param vsiz the size of the original record value.
   * @param xt the expiration time.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return the pointer to the result buffer.
   */
  static char* make_record_value(const char* vbuf, size_t vsiz, int64_t xt, size_t* sp) {
    _assert_(vbuf && vsiz <= kc::MEMMAXSIZ);
    size_t jsiz = vsiz + XTWIDTH;
    char* jbuf = new char[jsiz];
    kc::writefixnum(jbuf, xt, XTWIDTH);
    std::memcpy(jbuf + XTWIDTH, vbuf, vsiz);
    *sp = jsiz;
    return jbuf;
  }
  /**
   * Modify an expiration time by the current time.
   * @param xt the expiration time.
   * @param ct the current time.
   * @return the modified expiration time.
   */
  static int64_t modify_exptime(int64_t xt, int64_t ct) {
    _assert_(true);
    if (xt < 0) {
      if (xt < INT64_MIN / 2) xt = INT64_MIN / 2;
      xt = -xt;
    } else {
      if (xt > INT64_MAX / 2) xt = INT64_MAX / 2;
      xt += ct;
    }
    if (xt > XTMAX) xt = XTMAX;
    return xt;
  }
  /** The expiration cursor lock. */
  kc::SpinLock xlock_;
  /** The internal database. */
  kc::PolyDB db_;
  /** The open mode. */
  uint32_t omode_;
  /** The options. */
  uint8_t opts_;
  /** The capacity of record number. */
  int64_t capcnt_;
  /** The capacity of memory usage. */
  int64_t capsiz_;
  /** The cursor for expiration. */
  kc::PolyDB::Cursor* xcur_;
  /** The score of expiration. */
  kc::AtomicInt64 xsc_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
