import scala.reflect.ClassTag

case class Hola(pepe: String, jamon: Int)

Hola("capo", 23).getClass.getConstructors

def pepe[T <: Product](implicit ev: ClassTag[T]) = {

  println(ev.runtimeClass.getInterfaces.toList)
  println(ev.runtimeClass.getDeclaredFields.toList)

}

pepe[Hola]
