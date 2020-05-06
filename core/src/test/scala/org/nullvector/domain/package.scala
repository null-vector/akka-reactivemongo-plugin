package org.nullvector

package object domain {

  case class A(b: B, c: C, d: D, js: Seq[J])

  case class B(f: Set[F], g: G)

  case class C(s: String, m: Map[String, Seq[J]])

  case class D(i: Int, m: Map[String, H] = Map.empty)

  case class F(maybeC: Option[C])

  case class G(ds: List[D])

  case class H(d: BigDecimal)

  case class J(s: String)

  case class I(k: K)

  case class K(s: String)

  case class L(m: Map[Day, String], day: Day)

  sealed trait Day

  object Day {
    def apply(name: String): Day = name match {
      case "Monday" => Monday
      case "Sunday" => Sunday
    }
  }


  case object Monday extends Day

  case object Sunday extends Day


  case class Money(amount: BigDecimal, currency: Money.Currency) {

    def +(aMoney: Money): Money = copy(amount + aMoney.amount)

    def *(factor: BigDecimal): Money = copy(amount * factor)
  }

  object Money extends Enumeration {
    type Currency = Value
    val ARS, BRL, USD, MXN = Value

    def ars(amount: BigDecimal): Money = Money(amount, ARS)
  }

  case class Product(name: String, unitPrice: Money)

  final case class OrderId(val id: Int) extends AnyVal
  case class Order(orderId: OrderId, products: Seq[Product])

}
