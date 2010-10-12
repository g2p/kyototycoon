package kyototycoon;


/**
* kyototycoon/CursorOperations.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public interface CursorOperations 
{
  boolean set_value (String value, long xt, boolean step);
  boolean remove ();
  String get_key (boolean step);
  String get_value (boolean step);
  boolean jump ();
  boolean jump_ (String key);
  boolean jump_back ();
  boolean jump_back_ (String key);
  boolean step ();
  boolean step_back ();
  kyototycoon.DB db ();
  kyototycoon.Error error ();
} // interface CursorOperations
