package org.nullvector.query

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.query.{javadsl, scaladsl, _}
import org.nullvector.UnderlyingPersistenceFactory

object ReactiveMongoJournalProvider extends ExtensionId[ReactiveMongoJournalProvider] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] =
    ReactiveMongoJournalProvider

  override def createExtension(
      system: ExtendedActorSystem
  ): ReactiveMongoJournalProvider = new ReactiveMongoJournalProvider(system)
}

class ReactiveMongoJournalProvider(system: ExtendedActorSystem) extends ReadJournalProvider with Extension {

  import akka.actor.typed.scaladsl.adapter._

  private val scalaReadJournal: ReactiveMongoScalaReadJournal =
    createUnderlyingFactory(Nil)

  // Akka defines these as methods with an empty parameter list; Scala 3 requires the ().
  override def scaladslReadJournal(): scaladsl.ReadJournal =
    scalaReadJournal

  def scalaReadJournalTyped: ReactiveMongoScalaReadJournal =
    scalaReadJournal

  /** Creates a ReadJournal that apply queries only on given entities
    * @param entitiesNames
    *   these names will be mapped to MongoDB collections.
    * @return
    */
  def readJournalFor(entitiesNames: List[String]): ReactiveMongoScalaReadJournal = createUnderlyingFactory(
    entitiesNames
  )

  private def createUnderlyingFactory(names: List[String]) = {
    UnderlyingPersistenceFactory(
      new ReactiveMongoScalaReadJournalImpl(system, names),
      new FromMemoryReadJournal(system.toTyped)
    )(system)
  }

  private val javaReadJournal: ReactiveMongoJavaReadJournal =
    new ReactiveMongoJavaReadJournal(scalaReadJournal)

  override def javadslReadJournal(): javadsl.ReadJournal =
    javaReadJournal

  def javaReadJournalTyped: ReactiveMongoJavaReadJournal =
    javaReadJournal
}
