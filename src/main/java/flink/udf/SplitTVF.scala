package flink.udf

import org.apache.flink.table.functions.TableFunction

class SplitTVF extends TableFunction[SimpleUser] {

  def eval(user: String): Unit = {
    if (user.contains("#")) {
      val splits = user.split("#")
      collect(SimpleUser(splits(0), splits(1).toInt))
    }
  }
}
