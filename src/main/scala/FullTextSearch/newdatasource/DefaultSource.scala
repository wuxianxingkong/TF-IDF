package FullTextSearch.newdatasource

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by cuiguangfan on 16-10-25.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null);
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.getOrElse("path",sys.error("'path' must be specified for our data."))
    return new LegacyRelation(parameters.get("path").get,schema)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    saveAsCsvFile(data,parameters.get("path").get)
    createRelation(sqlContext,parameters,data.schema)
  }
  def saveAsCsvFile(data:DataFrame,path:String)={
    val dataCustomRDD=data.rdd.map(row=>{
      val values=row.toSeq.map(value=>value.toString)
      values.mkString(",")
    })
    dataCustomRDD.saveAsTextFile(path)
  }
}
