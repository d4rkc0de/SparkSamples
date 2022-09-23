package org.d4rkc0de.helper
import scala.reflect.runtime.universe._

object Utils {
  def getMethods[T: TypeTag]: List[String] =
    typeOf[T].members.sorted.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
    }
}
