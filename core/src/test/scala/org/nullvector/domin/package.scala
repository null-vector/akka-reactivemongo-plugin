package org.nullvector

package object domin {

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

  case class PlanetDistanceBetweenEarth(and: SolarPlanet, kilometers: Double)

  sealed trait SolarPlanet

  case object Mercury extends SolarPlanet

  case object Venus extends SolarPlanet

  case object Earth extends SolarPlanet

  case object Mars extends SolarPlanet

  case object Jupiter extends SolarPlanet

  case object Saturn extends SolarPlanet

  case object Uranus extends SolarPlanet

  case object Neptune extends SolarPlanet

}
