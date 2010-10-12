package kyototycoon;

/**
* kyototycoon/ListHolder.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public final class ListHolder implements org.omg.CORBA.portable.Streamable
{
  public kyototycoon.List value = null;

  public ListHolder ()
  {
  }

  public ListHolder (kyototycoon.List initialValue)
  {
    value = initialValue;
  }

  public void _read (org.omg.CORBA.portable.InputStream i)
  {
    value = kyototycoon.ListHelper.read (i);
  }

  public void _write (org.omg.CORBA.portable.OutputStream o)
  {
    kyototycoon.ListHelper.write (o, value);
  }

  public org.omg.CORBA.TypeCode _type ()
  {
    return kyototycoon.ListHelper.type ();
  }

}
