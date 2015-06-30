package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy._


object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class PersistenceTimeout(persist: Persistence.Persist)
  case class OperationTimeout(id: Long)

  case class PendingOperation(
      client: ActorRef,
      id: Long,
      tick: Cancellable,
      persistencePending: Boolean,
      pendingReplicators: Set[ActorRef]
  ) {

    def isFinish: Boolean = {
      !persistencePending && pendingReplicators.isEmpty
    }

  }

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var currentSeq = 0

  var persistence = initPersistence()

  val allReplicates = collection.mutable.LinkedHashMap.empty[Long, Replicate]

  //operations which waiting for persistence or replica response
  var pendingOperations = Map.empty[Long, PendingOperation]


  private def initPersistence(): ActorRef = {
    context.watch(context.system.actorOf(persistenceProps))
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    arbiter ! Join
  }


  def receive = {
    case JoinedPrimary   => context.become(leader orElse common)
    case JoinedSecondary => context.become(replica orElse common)
  }

  val common: Receive = {
    case Terminated(_) =>
      initPersistence()
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case operation @ Insert(k, v, id) =>
      kv += k -> v
      sendPersist(Persist(k, Some(v), id))
      sendReplicate(Replicate(k, Some(v), id))
      addPendingOperation(sender, operation)

    case operation @ Remove(k, id) =>
      kv -= k
      sendPersist(Persist(k, None, id))
      sendReplicate(Replicate(k, None, id))
      addPendingOperation(sender, operation)

    case Get(k, id) =>
      sender ! GetResult(k, kv.get(k), id)

    case Persisted(k, id) =>
      updatePendingOperation(id) { po =>
        po.copy(persistencePending = false)
      }

    case Replicated(k, id) =>
      updatePendingOperation(id) { po =>
        po.copy(pendingReplicators = po.pendingReplicators - sender)
      }

    case pt: PersistenceTimeout =>
      if(pendingOperations.contains(pt.persist.id)) {
        sendPersist(pt.persist)
      }

    case OperationTimeout(id) =>
      pendingOperations.get(id).foreach { po =>
        po.client ! OperationFailed(id)
        pendingOperations -= id
      }

    case Replicas(replicas) =>
      val secondReplicas = replicas.filter(_ != self)
      val newlyReplicas = secondReplicas.filter(!secondaries.keySet.contains(_))
      val removedReplicas = secondaries.keySet.filter(!secondReplicas.contains(_))
      newlyReplicas foreach { replica =>
        val replicator = context.actorOf(Replicator.props(replica))
        replicators += replicator
        secondaries += replica -> replicator
        //send inital state to newly joined replica
        allReplicates.foreach {case (k, v) => replicator ! v}
      }
      var removedReplicators = Set.empty[ActorRef]
      removedReplicas foreach { replica =>
        secondaries.get(replica).foreach { replicator =>
          replicator ! PoisonPill
          secondaries -= replica
          replicators -= replicator
          removedReplicators += replicator
        }
      }
      //waive outstanding acknowledgements of these replicas
      if(removedReplicators.nonEmpty) {
        val needCheckMap = pendingOperations.filter { case (k, v) =>
          (v.pendingReplicators & removedReplicators).nonEmpty
        }
        needCheckMap.foreach { case (id, pos) =>
          removedReplicators.foreach { replicator =>
            if(pos.pendingReplicators.contains(replicator)) {
              updatePendingOperation(id) { po =>
                po.copy(pendingReplicators = po.pendingReplicators - replicator)
              }
            }
          }
        }
      }

  }


  def updatePendingOperation(id: Long)(f: (PendingOperation) => PendingOperation): Unit = {
    pendingOperations.get(id).foreach { po =>
      val newPo = f(po)
      if (newPo.isFinish) {
        newPo.tick.cancel()
        newPo.client ! OperationAck(id)
        pendingOperations -= id
      } else {
        pendingOperations += id -> newPo
      }
    }
  }

  private def addPendingOperation(sender: ActorRef, operation: Operation): Unit = {
    pendingOperations += operation.id -> PendingOperation(
      sender,
      operation.id,
      context.system.scheduler.scheduleOnce(1000.millisecond, self, OperationTimeout(operation.id)),
      persistencePending = true,
      replicators
    )

  }

  private def sendReplicate(replicate: Replicate): Unit = {
    allReplicates += replicate.id -> replicate
    replicators foreach (_ ! replicate)
  }


  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(k, id) =>
      sender ! GetResult(k, kv.get(k), id)

    case Snapshot(k, v, seq) =>
      replicators += sender
      if(seq < currentSeq) {
        sender ! SnapshotAck(k, seq)
      } else if(seq == currentSeq) {
        v.fold(kv -= k)(kv += k -> _)
        sendPersist(Persist(k, v, seq))
      }

    case Persisted(k, seq) =>
      replicators.foreach(_ ! SnapshotAck(k, seq))  //exactly one replicator
      currentSeq += 1

    case pt: PersistenceTimeout =>
      if(pt.persist.id >= currentSeq) {
        sendPersist(pt.persist)
      }
  }

  private def sendPersist(persist: Persist) = {
    persistence ! persist
    context.system.scheduler.scheduleOnce(100.millisecond, self, PersistenceTimeout(persist))
  }

}

