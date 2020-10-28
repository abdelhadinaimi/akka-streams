import java.nio.file.Paths

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object Main extends App {
  implicit val system: ActorSystem = ActorSystem("QuickStart")

  def lineSink[T](filename: String): Sink[T, Future[IOResult]] =
    Flow[T].map(s => ByteString(s + "\n")).toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)

  val idxSink = lineSink[Int]("idx.txt")
  val factorialSink = lineSink[BigInt]("factorials.txt")

  val source1: Source[Int, NotUsed] = Source(1 to 10)
  val source2: Source[Int, NotUsed] = Source(10 to 12)

  val factorialFlow: Flow[Int, BigInt, NotUsed] = Flow[Int].scan(BigInt(1))((acc, next) => acc * next)

  def zippedFactorialFlow(source: Source[Int, NotUsed]): Flow[BigInt, (Int, BigInt), NotUsed] =
    Flow[BigInt].zipWith(source)((num, idx) => (idx, num))

  type T = (Int, BigInt)

  def zipFactorialGraph(source: Source[Int, NotUsed]): Graph[UniformFanInShape[Int, (Int, BigInt)], NotUsed] =
    GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._
    val fac = b.add(factorialFlow)
    val zip = b.add(zippedFactorialFlow(source))
    fac ~> zip
    UniformFanInShape(zip.out, fac.in)
  }

  val g = RunnableGraph.fromGraph(GraphDSL.create(idxSink, factorialSink)((_, _)) { implicit b => (idOut, factorialOut) =>
    import GraphDSL.Implicits._

    val bcast = b.add(Broadcast[T](2))
    val merge = b.add(Merge[T](2))
    val zipFactorialPart = b.add(zipFactorialGraph(source1))

    source1 ~> zipFactorialPart ~> bcast.in
    source2 ~> factorialFlow ~> zippedFactorialFlow(source2) ~> merge
    bcast.out(0) ~> Flow[T].map(_._1) ~> idOut
    bcast.out(1) ~> merge ~> Flow[T].map(_._2) ~> factorialOut

    ClosedShape
  })
  g.run()
}
