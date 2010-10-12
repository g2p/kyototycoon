package kyototycoon;

/**
* kyototycoon/DBHolder.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public final class DBHolder implements org.omg.CORBA.portable.Streamable
{
  public kyototycoon.DB value = null;

  public DBHolder ()
  {
  }

  public DBHolder (kyototycoon.DB initialValue)
  {
    value = initialValue;
  }

  public void _read (org.omg.CORBA.portable.InputStream i)
  {
    value = kyototycoon.DBHelper.read (i);
  }

  public void _write (org.omg.CORBA.portable.OutputStream o)
  {
    kyototycoon.DBHelper.write (o, value);
  }

  public org.omg.CORBA.TypeCode _type ()
  {
    return kyototycoon.DBHelper.type ();
  }

}
