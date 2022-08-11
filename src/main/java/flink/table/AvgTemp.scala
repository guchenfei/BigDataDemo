package flink.table

import org.apache.flink.table.functions.AggregateFunction

class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count
  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  def accumulate(accumulator: AvgTempAcc, temp: String): Unit ={
    accumulator.sum += temp.toDouble
    accumulator.count += 1
  }
}
