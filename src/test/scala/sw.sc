import scala.reflect.ClassTag

case class Hola(pepe: String, jamon: Int)

Hola("capo", 23).getClass.getConstructors

def pepe[T <: Product](implicit ev: ClassTag[T]) = {

  println(ev.runtimeClass.getInterfaces.toList)
  println(ev.runtimeClass.getDeclaredFields.toList)

}

pepe[Hola]

case class Pepe(list: List[Int])

val bool = classOf[List[Int]].isAssignableFrom(List(1, 2, 3).getClass)



