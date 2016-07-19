package com.sclasen.akka.kafka

import akka.actor._
import concurrent.duration._
import kafka.consumer._

import com.sclasen.akka.kafka.ConnectorFSM._
import StreamFSM._
import kafka.message.MessageAndMetadata

object ConnectorFSM {

  sealed trait ConnectorState //连接状态

  case object Committing extends ConnectorState //提交

  case object Receiving extends ConnectorState //接收

  case object Stopped extends ConnectorState //停止

  sealed trait ConnectorProtocol  //连接协议

  case object Start extends ConnectorProtocol //开始

  case class Drained(stream: String) extends ConnectorProtocol //耗尽

  case object Received extends ConnectorProtocol //接收到

  case object Commit extends ConnectorProtocol //提交

  case object Started extends ConnectorProtocol //已开始

  case object Committed extends ConnectorProtocol //已提交

  case object Stop extends ConnectorProtocol //停止

}

object StreamFSM {

  sealed trait StreamState

  case object Processing extends StreamState

  case object Full extends StreamState

  case object Draining extends StreamState

  case object Empty extends StreamState

  case object Unused extends StreamState

  case object FlattenContinue extends StreamState

  sealed trait StreamProtocol

  case object StartProcessing extends StreamProtocol //开始处理消息

  case object Drain extends StreamProtocol //已消耗消息

  case object Processed extends StreamProtocol

  case object Continue extends StreamProtocol

  case object Stop extends StreamProtocol

}

class ConnectorFSM[Key, Msg](props: AkkaConsumerProps[Key, Msg], connector: ConsumerConnector) extends Actor with FSM[ConnectorState, Int] {

  import props._
  import context.dispatcher

  /*
   * start with Stopped , cnt 0 of ConnectorState[Committing|Receiving|Stopped]
   */
  startWith(Stopped, 0)

  var commitTimeoutCancellable: Option[Cancellable] = None
  var committer: Option[ActorRef] = None

  def scheduleCommit = { //延迟
    commitTimeoutCancellable = commitConfig.commitInterval.map(i => context.system.scheduler.scheduleOnce(i, self, Commit))
  }

  when(Stopped) {
    case Event(Start, _) =>
      log.info("at=start")
      def startTopic(topic:String){
        connector.createMessageStreams(Map(topic -> streams), props.keyDecoder, props.msgDecoder).apply(topic).zipWithIndex.foreach {
          case (stream, index) =>
            context.actorOf(Props(new StreamFSM(stream, maxInFlightPerStream, receiver, msgHandler)), s"stream${index}")
        }
      }
      def startTopicFilter(topicFilter:TopicFilter){
        connector.createMessageStreamsByFilter(topicFilter, streams, props.keyDecoder, props.msgDecoder).zipWithIndex.foreach {
          case (stream, index) =>
            context.actorOf(Props(new StreamFSM(stream, maxInFlightPerStream, receiver, msgHandler)), s"stream${index}")
        }
      }

      // 根据构建参数启动topicFilter or startTopic
      topicFilterOrTopic.fold(startTopicFilter, startTopic)

      log.info("at=created-streams")
      context.children.foreach(_ ! Continue)  //发送 Continue 指令 给 StreamFSM children
      scheduleCommit
      sender ! Started  //已开始 好像没什么用
      goto(Receiving) using 0 //跳转到接收状态
  }

  when(Receiving) {
    case Event(Received, uncommitted) if commitConfig.commitAfterMsgCount.exists(_ == uncommitted) =>
      debugRec(Received, uncommitted)
      goto(Committing) using 0
    case Event(Received, uncommitted) =>
      debugRec(Received, uncommitted + 1)
      stay using (uncommitted + 1)
    case Event(Commit, uncommitted) =>
      debugRec(Commit, uncommitted)
      committer = Some(sender)
      goto(Committing) using 0  // 转到Committing状态
    case Event(Committed, uncommitted) => //收到已提交Committed 消息
      debugRec(Committed, uncommitted)
      stay()
    case Event(d@Drained(s), uncommitted) =>
      debugRec(d, uncommitted) /*when we had to send more than 1 Drain msg to streams, we get these*/
      stay()
  }

  onTransition {
    case Receiving -> Committing =>
      log.info("at=transition from={} to={} uncommitted={}", Receiving, Committing, stateData)
      commitTimeoutCancellable.foreach(_.cancel())  //停止commitTimeout
      context.children.foreach(_ ! Drain) //向StreamFSM 发送Drain消息
  }

  onTransition {
    case Committing -> Receiving =>
      log.info("at=transition from={} to={}", Committing, Receiving)
      scheduleCommit //设置间隔发送Commit消息
      committer.foreach(_ ! Committed) //好像也没什么作用
      committer = None
      context.children.foreach(_ ! StartProcessing) //向StreamFSM 发送StartProcessing消息
  }

  when(Committing, stateTimeout = 1 seconds) { //提交offsets中
    case Event(Received, drained) =>
      debugCommit(Received, "stream", drained)
      stay()
    case Event(StateTimeout, drained) =>
      log.warning("state={} msg={} drained={} streams={}", Committing, StateTimeout, drained, streams)
      context.children.foreach(_ ! Drain)
      stay using (0)
    case Event(d@Drained(stream), drained) if drained + 1 < context.children.size =>
      debugCommit(d, stream, drained + 1)
      stay using (drained + 1)
    case Event(d@Drained(stream), drained) if drained + 1 == context.children.size =>
      debugCommit(d, stream, drained + 1)
      log.info("at=drain-finished")
      connector.commitOffsets
      log.info("at=committed-offsets")
      goto(Receiving) using 0
  }

  whenUnhandled{
    case Event(ConnectorFSM.Stop, _) =>
      connector.shutdown()
      sender() ! ConnectorFSM.Stop
      context.children.foreach(_ ! StreamFSM.Stop)
      stop()
  }

  def debugRec(msg:AnyRef, uncommitted:Int) = log.debug("state={} msg={} uncommitted={}", Receiving, msg,  uncommitted)

  def debugCommit(msg:AnyRef, stream:String, drained:Int) = log.debug("state={} msg={} drained={}", Committing, msg,  drained)

}

class StreamFSM[Key, Msg](stream: KafkaStream[Key, Msg], maxOutstanding: Int, receiver: ActorRef, msgHandler: (MessageAndMetadata[Key,Msg]) => Any) extends Actor with FSM[StreamState, Int] {

  lazy val msgIterator = stream.iterator()
  val conn = context.parent //父节点是ConnectorFSM

  def hasNext() = try {
    msgIterator.hasNext()
  } catch {
    case cte: ConsumerTimeoutException => false
  }

  //初始Processing 状态
  startWith(Processing, 0)

  when(Processing) {
    /* too many outstanding, wait */
    case Event(Continue, outstanding) if outstanding == maxOutstanding =>
       debug(Processing, Continue, outstanding)
       goto(Full)
    /* ok to process, and msg available */
    case Event(Continue, outstanding) if hasNext() =>
      val msg = msgHandler(msgIterator.next())
      conn ! Received
      debug(Processing, Continue, outstanding +1)
      receiver ! msg
      self ! Continue
      stay using (outstanding + 1)
    /* no message in iterator and no outstanding. this stream is prob not going to get messages */
    case Event(Continue, outstanding) if outstanding == 0 =>
      debug(Processing, Continue, outstanding)
      goto(Unused)
    /* no msg in iterator, but have outstanding */
    case Event(Continue, outstanding) =>
      debug(Processing, Continue, outstanding)
      goto(FlattenContinue)
    /* message processed */
    case Event(Processed, outstanding) =>
      debug(Processing, Processed, outstanding -1)
      self ! Continue
      stay using (outstanding - 1)
    /* conn says drain, we have no outstanding */
    case Event(Drain, outstanding) if outstanding == 0 => //未完成数量为0时
      debug(Processing, Drain, outstanding)
      goto(Empty)
    /* conn says drain, we have outstanding */
    case Event(Drain, outstanding) =>
      debug(Processing, Drain, outstanding)
      goto(Draining)
  }

  /*
  We go into FlattenContinue when we poll the iterator and there are no msgs but we have in-flight messages.
  We anticipate that future calls to the iterator will timeout as well, so instead of polling once for every Continue
  that is in the mailbox, we just drain them out, and wait for the in-flight messages to drain.
  once we do that, we go back to processing. This speeds up commit alot in the cycle where a stream goes empty.
  */
  when(FlattenContinue){
    case Event(Continue, outstanding) =>
      debug(FlattenContinue, Continue, outstanding)
      stay()
    case Event(Processed, outstanding) if outstanding == 1 =>
      debug(FlattenContinue, Processed, outstanding -1)
      self ! Continue
      goto(Processing) using (outstanding - 1)
    case Event(Processed, outstanding) =>
      debug(FlattenContinue, Processed, outstanding -1)
      stay using (outstanding - 1)
    case Event(Drain, outstanding) if outstanding == 0 =>
      debug(FlattenContinue, Drain, outstanding)
      goto(Empty)
    case Event(Drain, outstanding) =>
      debug(FlattenContinue, Drain, outstanding)
      goto(Draining)
  }

  when(Full) {
    case Event(Continue, outstanding) =>
      debug(Full, Continue, outstanding)
      stay()
    case Event(Processed, outstanding) =>
      debug(Full, Processed, outstanding - 1)
      goto(Processing) using (outstanding - 1)
    case Event(Drain, outstanding) =>
      debug(Full, Drain, outstanding)
      goto(Draining)
  }

  when(Draining) {
    /* drained last message */
    case Event(Processed, outstanding) if outstanding == 1 =>
      debug(Draining, Processed, outstanding)
      goto(Empty) using 0
    /* still draining  */
    case Event(Processed, outstanding) =>
      debug(Draining, Processed, outstanding)
      stay using (outstanding - 1)
    case Event(Continue, outstanding) =>
      debug(Draining, Continue, outstanding)
      stay()
    case Event(Drain, outstanding) =>
      debug(Draining, Drain, outstanding)
      stay()
  }

  when(Empty) {
    /* conn says go */
    case Event(StartProcessing, outstanding) => //收到开始处理消息
      debug(Unused, Drain, outstanding)
      goto(Processing) using 0
    case Event(Continue, outstanding) =>
      debug(Unused, Drain, outstanding)
      stay()
    case Event(Drain, _) =>
      conn ! Drained(me)
      stay()
  }

  /* we think this stream wont get messages */
  when(Unused) {
    case Event(Drain, outstanding) =>
      debug(Unused, Drain, outstanding)
      goto(Empty)
    case Event(Continue, outstanding) =>
      debug(Unused, Continue, outstanding)
      goto(Processing)
  }

  whenUnhandled{
    case Event(StreamFSM.Stop, _) =>
      stop()
  }

  onTransition {
    case Empty -> Processing =>
      self ! Continue
  }

  onTransition {
    case Full -> Processing =>
      self ! Continue
  }

  onTransition {
    case Processing -> Full =>
      self ! Continue
  }

  onTransition {
    case _ -> Empty =>
      log.debug("stream={} at=Drained", me)
      conn ! Drained(me) //到Empty状态时，向conn发送Drained
  }

  onTransition(handler _)

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "message {}", message)
    super.preRestart(reason, message)
  }

  def handler(from: StreamState, to: StreamState) {
    log.debug("stream={} at=transition from={} to={}", me, from, to)
  }

  def debug(state:StreamState, msg:StreamProtocol, outstanding:Int) = log.debug("stream={} state={} msg={} outstanding={}", me, state, msg,  outstanding)

  lazy val me = self.path.name
}
