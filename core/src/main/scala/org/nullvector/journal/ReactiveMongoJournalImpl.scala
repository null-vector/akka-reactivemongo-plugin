package org.nullvector.journal

import org.nullvector.ReactiveMongoPlugin

trait ReactiveMongoJournalImpl extends ReactiveMongoPlugin
  with ReactiveMongoAsyncWrite
  with ReactiveMongoAsyncReplay
  with ReactiveMongoHighestSequence
  with ReactiveMongoAsyncDeleteMessages {

}
