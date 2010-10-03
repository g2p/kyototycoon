#include <ktremotedb.h>

using namespace std;
using namespace kyototycoon;

// main routine
int main(int argc, char** argv) {

  // create the database object
  RemoteDB db;

  // open the database
  if (!db.open()) {
    cerr << "open error: " << db.error().name() << endl;
  }

  // store records
  if (!db.set("foo", "hop") ||
      !db.set("bar", "step") ||
      !db.set("baz", "jump")) {
    cerr << "set error: " << db.error().name() << endl;
  }

  // retrieve a record
  string* value = db.get("foo");
  if (value) {
    cout << *value << endl;
    delete value;
  } else {
    cerr << "get error: " << db.error().name() << endl;
  }

  // traverse records
  RemoteDB::Cursor* cur = db.cursor();
  cur->jump();
  pair<string, string>* rec;
  while ((rec = cur->get_pair(NULL, true)) != NULL) {
    cout << rec->first << ":" << rec->second << endl;
    delete rec;
  }
  delete cur;

  // close the database
  if (!db.close()) {
    cerr << "close error: " << db.error().name() << endl;
  }

  return 0;
}
