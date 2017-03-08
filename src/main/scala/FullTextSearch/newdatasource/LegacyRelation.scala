package FullTextSearch.newdatasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._

/**
  * Created by cuiguangfan on 16-10-25.
  */
class LegacyRelation(location: String,userSchema: StructType)(@transient val sqlContext:SQLContext) extends BaseRelation with TableScan with Serializable{
  override def schema: StructType = {
    if(this.userSchema!=null){
      return this.userSchema
    }else {
      return StructType(Seq(StructField("name",StringType,true),StructField("age",IntegerType,true),StructField("live",StringType,true),StructField("born",StringType,true)))
    }
  }

  override def buildScan(): RDD[Row] = {
    val schemaFields=schema.fields
    val rdd=sqlContext.sparkContext.wholeTextFiles(location).map(x=>x._2)
    val rows=rdd.map(file => {
      val lines=file.split("\n")
      lines.zipWithIndex.map(println)
      val typedValues=lines.zipWithIndex.map{
        case(value,index)=>{
          val dataType=schemaFields(index).dataType
          castValue(value,dataType)
        }
      }
      Row.fromSeq(typedValues)
    })
    rows
  }
  private def castValue(value: String,toType:DataType)=toType match{
    case _:StringType => value
    case _:IntegerType=>value.toInt
  }
}
