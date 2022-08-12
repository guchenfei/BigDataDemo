package flink.table

import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

class Top2Temp extends TableAggregateFunction[(Double,Int),Top2TempAcc]{
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

  def accumulate(acc : Top2TempAcc,temp:String):Unit = {
    if (temp.toDouble > acc.highestTmp){
      acc.secondHighestTemp = acc.highestTmp
      acc.highestTmp = temp.toDouble
    }else if (temp.toDouble > acc.secondHighestTemp){
      acc.secondHighestTemp = temp.toDouble
    }
  }

  def emitValue(acc:Top2TempAcc,out:Collector[(Double,Int)]): Unit ={
    out.collect(acc.highestTmp,1)
    out.collect(acc.secondHighestTemp,2)
  }
}
