package kyototycoon;


/**
* kyototycoon/MapHelper.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
abstract public class MapHelper
{
  private static String  _id = "IDL:kyototycoon/Map:1.0";

  public static void insert (org.omg.CORBA.Any a, kyototycoon.Map that)
  {
    org.omg.CORBA.portable.OutputStream out = a.create_output_stream ();
    a.type (type ());
    write (out, that);
    a.read_value (out.create_input_stream (), type ());
  }

  public static kyototycoon.Map extract (org.omg.CORBA.Any a)
  {
    return read (a.create_input_stream ());
  }

  private static org.omg.CORBA.TypeCode __typeCode = null;
  synchronized public static org.omg.CORBA.TypeCode type ()
  {
    if (__typeCode == null)
    {
      __typeCode = org.omg.CORBA.ORB.init ().create_interface_tc (kyototycoon.MapHelper.id (), "Map");
    }
    return __typeCode;
  }

  public static String id ()
  {
    return _id;
  }

  public static kyototycoon.Map read (org.omg.CORBA.portable.InputStream istream)
  {
    return narrow (istream.read_Object (_MapStub.class));
  }

  public static void write (org.omg.CORBA.portable.OutputStream ostream, kyototycoon.Map value)
  {
    ostream.write_Object ((org.omg.CORBA.Object) value);
  }

  public static kyototycoon.Map narrow (org.omg.CORBA.Object obj)
  {
    if (obj == null)
      return null;
    else if (obj instanceof kyototycoon.Map)
      return (kyototycoon.Map)obj;
    else if (!obj._is_a (id ()))
      throw new org.omg.CORBA.BAD_PARAM ();
    else
    {
      org.omg.CORBA.portable.Delegate delegate = ((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate ();
      kyototycoon._MapStub stub = new kyototycoon._MapStub ();
      stub._set_delegate(delegate);
      return stub;
    }
  }

  public static kyototycoon.Map unchecked_narrow (org.omg.CORBA.Object obj)
  {
    if (obj == null)
      return null;
    else if (obj instanceof kyototycoon.Map)
      return (kyototycoon.Map)obj;
    else
    {
      org.omg.CORBA.portable.Delegate delegate = ((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate ();
      kyototycoon._MapStub stub = new kyototycoon._MapStub ();
      stub._set_delegate(delegate);
      return stub;
    }
  }

}
