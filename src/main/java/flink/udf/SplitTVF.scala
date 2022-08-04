package flink.udf

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{FunctionParameter, TableFunction}

import java.lang.reflect.Type
import java.util

class SplitTVF extends TableFunction[SimpleUser] {
  override def getRowType(relDataTypeFactory: RelDataTypeFactory, list: util.List[_]): RelDataType = {
    null
  }

  override def getElementType(list: util.List[_]): Type = {
    null
  }

  override def getParameters: util.List[FunctionParameter] = {
    null
  }

  def collect(user: SimpleUser) = {}

  def eval(user: String): Unit = {
    if (user.contains("#")) {
      val splits = user.split("#")
      collect(SimpleUser(splits(0), splits(1).toInt))
    }
  }
}
