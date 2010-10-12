package kyototycoon;

/**
* kyototycoon/CursorHolder.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public final class CursorHolder implements org.omg.CORBA.portable.Streamable
{
  public kyototycoon.Cursor value = null;

  public CursorHolder ()
  {
  }

  public CursorHolder (kyototycoon.Cursor initialValue)
  {
    value = initialValue;
  }

  public void _read (org.omg.CORBA.portable.InputStream i)
  {
    value = kyototycoon.CursorHelper.read (i);
  }

  public void _write (org.omg.CORBA.portable.OutputStream o)
  {
    kyototycoon.CursorHelper.write (o, value);
  }

  public org.omg.CORBA.TypeCode _type ()
  {
    return kyototycoon.CursorHelper.type ();
  }

}
