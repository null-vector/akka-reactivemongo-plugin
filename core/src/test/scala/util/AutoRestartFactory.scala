package util

import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern._

class AutoRestartFactory(system: ActorSystem) {

  private val autoRestartRef: ActorRef = system.actorOf(Props(new AutoRestartSupervisor))

  def create(props: Props, name: String): ActorRef = {
    Await.result((autoRestartRef ? (props, name)) (Timeout(1.second)).mapTo[ActorRef], 1.second)
  }

  private class AutoRestartSupervisor() extends Actor {
    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
      case _ => SupervisorStrategy.Restart
    }

    override def receive: Receive = {
      case (props: Props, name: String) => sender() ! context.actorOf(props, name)
    }
  }

}
