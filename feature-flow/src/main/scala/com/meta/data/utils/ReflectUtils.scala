package com.meta.data.utils

import java.util.concurrent.ConcurrentHashMap

import scala.reflect.runtime.universe.MethodSymbol
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe

/**
 *
 *
 * @author weitaoliang 
 * @version V1.0 
 */
object ReflectUtils {
  // scala Object 调用方法
  // JavaMirror
  val classMirror = universe.runtimeMirror(getClass.getClassLoader)
  def reflectMethodInvoke(classPath: String, methodName: String, parameters: Array[Any]): Any = {
    val staticMirror = classMirror.staticModule(classPath)
    // ModuleMirror can be used to create instances of the class
    val moduleMirror = classMirror.reflectModule(staticMirror)
    // ObjectMirror can be used to reflect the members of the object
    val objectMirror = classMirror.reflect(moduleMirror.instance)
    val method = moduleMirror.symbol.typeSignature.member(universe.TermName(methodName)).asMethod
    objectMirror.reflectMethod(method)(parameters: _*)
  }
}
