package org.nullvector.domain

package object planets {

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
