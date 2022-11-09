package com.meta.data.conf

/**
 * 特征处理方法包装类，这里只包装了9个参数长度的方法，一般可以覆盖所有算法业务逻辑
 *
 * @author: weitaoliang
 * @version v1.0
 * */
// 方法复杂度超了10,业务需要屏蔽相关语法提醒
// scalastyle:off
class MethodWrapper(name: String,
                    f: AnyRef,
                    val alias: String) extends Serializable {
  def eval(params: Array[Any]): Any = {
    params.length match {
      case 0 =>
        val func = f.asInstanceOf[() => Any]
        func()
      case 1 =>
        val func = f.asInstanceOf[(Any) => Any]
        func(params(0))
      case 2 =>
        val func = f.asInstanceOf[(Any, Any) => Any]
        func(params(0), params(1))
      case 3 =>
        val func = f.asInstanceOf[(Any, Any, Any) => Any]
        func(params(0), params(1), params(2))
      case 4 =>
        val func = f.asInstanceOf[(Any, Any, Any, Any) => Any]
        func(params(0), params(1), params(2), params(3))
      case 5 =>
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
        func(params(0), params(1), params(2), params(3), params(4))
      case 6 =>
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
        func(params(0), params(1), params(2), params(3), params(4), params(5))
      case 7 =>
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
        func(params(0), params(1), params(2), params(3), params(4), params(5), params(6))
      case 8 =>
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        func(params(0), params(1), params(2), params(3), params(4), params(5), params(6), params(7))
      case 9 =>
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        func(params(0), params(1), params(2), params(3), params(4), params(5), params(6), params(7), params(8))
    }
  }
}
