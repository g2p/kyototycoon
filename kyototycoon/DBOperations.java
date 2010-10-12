package kyototycoon;


/**
* kyototycoon/DBOperations.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public interface DBOperations 
{
  kyototycoon.Error error ();
  boolean open (String host, int port, double timeout);
  boolean close ();
  kyototycoon.Map report ();
  kyototycoon.Map play_script (String name, kyototycoon.Map args);
  kyototycoon.Map status ();
  boolean clear ();
  long count ();
  long size ();
  boolean set (String key, String value, long xt);
  boolean add (String key, String value, long xt);
  boolean replace (String key, String value, long xt);
  boolean append (String key, String value, long xt);
  long increment (String key, long num, long xt);
  double increment_double (String key, double num, long xt);
  boolean cas (String key, String oval, String nval, long xt);
  boolean remove (String key);
  String get (String key);
  long set_bulk (kyototycoon.Map recs, long xt);
  long remove_bulk (kyototycoon.List keys);
  kyototycoon.Map get_bulk (kyototycoon.List keys);
  boolean vacuum (long steps);
  void set_target (String expr);
  String expression ();
  kyototycoon.Cursor cursor ();
} // interface DBOperations
