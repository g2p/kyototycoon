package kyototycoon;


/**
* kyototycoon/Error.java .
* IDL-to-Java コンパイラ (ポータブル), バージョン "3.1" で生成
* 生成元: kyototycoon.idl
* 2010年10月11日 21時47分12秒 JST
*/


//----------------------------------------------------------------
public interface Error extends ErrorOperations, org.omg.CORBA.Object, org.omg.CORBA.portable.IDLEntity 
{
  public static final int SUCCESS = (int)(0);
  public static final int NOIMPL = (int)(1);
  public static final int INVALID = (int)(2);
  public static final int LOGIC = (int)(3);
  public static final int INTERNAL = (int)(4);
  public static final int NETWORK = (int)(5);
  public static final int EMISC = (int)(15);
} // interface Error
