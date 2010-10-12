package kyototycoon;

/**
* kyototycoon/ErrorHolder.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public final class ErrorHolder implements org.omg.CORBA.portable.Streamable
{
  public kyototycoon.Error value = null;

  public ErrorHolder ()
  {
  }

  public ErrorHolder (kyototycoon.Error initialValue)
  {
    value = initialValue;
  }

  public void _read (org.omg.CORBA.portable.InputStream i)
  {
    value = kyototycoon.ErrorHelper.read (i);
  }

  public void _write (org.omg.CORBA.portable.OutputStream o)
  {
    kyototycoon.ErrorHelper.write (o, value);
  }

  public org.omg.CORBA.TypeCode _type ()
  {
    return kyototycoon.ErrorHelper.type ();
  }

}
