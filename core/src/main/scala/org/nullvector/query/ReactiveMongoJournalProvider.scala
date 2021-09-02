package org.nullvector.query

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.query._
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

  override val scaladslReadJournal: ReactiveMongoScalaReadJournal =
    createUnderlyingFactory(Nil)

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

  override val javadslReadJournal: ReactiveMongoJavaReadJournal =
    new ReactiveMongoJavaReadJournal(scaladslReadJournal)
}
