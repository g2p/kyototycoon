package kyototycoon;

/**
* kyototycoon/MapHolder.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public final class MapHolder implements org.omg.CORBA.portable.Streamable
{
  public kyototycoon.Map value = null;

  public MapHolder ()
  {
  }

  public MapHolder (kyototycoon.Map initialValue)
  {
    value = initialValue;
  }

  public void _read (org.omg.CORBA.portable.InputStream i)
  {
    value = kyototycoon.MapHelper.read (i);
  }

  public void _write (org.omg.CORBA.portable.OutputStream o)
  {
    kyototycoon.MapHelper.write (o, value);
  }

  public org.omg.CORBA.TypeCode _type ()
  {
    return kyototycoon.MapHelper.type ();
  }

}
