package com.pactera.spark

import java.util.Properties
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: xinghua
  * param: 离线统计结果统计分析
  * Date： 2018/8/3 19:28
  **/
object UserCount {
  val sparkConf = new SparkConf().setAppName("userCount").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sc)

  /**
    * Author: xinghua
    * param: 程序入口
    * Date： 2018/8/4 3:16
    **/
  def main(args: Array[String]): Unit = {
    getHdfsData()
    getHbaseData()
    dataToHive()
    dataToMysql()
  }

  /**
    * 获取hdfs数据
    */
  def getHdfsData(): Unit = {

    //  将hdfs文件转换为RDD
    val textRDD: RDD[String] = sc.textFile(UserPropertis.getKeyValue("fileSrc"))
    //定义一个schemaString字符串，用于头信息
    val schemaString = "uid pageid actionid ext"
    //将rdd转换为row对象
    val userRow = textRDD.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3)))
    //将schemaString转换为schema信息
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    val scheme = StructType(schemaString.split(" ").map(fileName => StructField(fileName, StringType)))
    //创建dataframe
    val df = sqlContext.createDataFrame(userRow, scheme)
    //创建虚拟表
    df.registerTempTable("user")
  }

  /**
    * Author: xinghua
    * param: 获取hbase数据
    * Date： 2018/8/4 3:16
    **/
  def getHbaseData(): Unit = {
    import sqlContext.implicits._
    val conf = HBaseConfiguration.create()
    println("hbasezookeeper"+UserPropertis.getKeyValue("hbaseZk"))
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", UserPropertis.getKeyValue("hbaseZk"))
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", UserPropertis.getKeyValue("hbaseZkPort"))
    conf.set(TableInputFormat.INPUT_TABLE, UserPropertis.getKeyValue("hbaseTableName"))

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(UserPropertis.getKeyValue("hbaseTableName"))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(UserPropertis.getKeyValue("hbaseTableName")))
      admin.createTable(tableDesc)
    }
    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val hbaseTable = hBaseRDD.map(result => (
      Bytes.toString(result._2.getRow),
      Bytes.toString(result._2.getValue(Bytes.toBytes("0"), Bytes.toBytes("date"))),
      Bytes.toString(result._2.getValue(Bytes.toBytes("0"), Bytes.toBytes("country"))),
      Bytes.toString(result._2.getValue(Bytes.toBytes("0"), Bytes.toBytes("province"))),
      Bytes.toString(result._2.getValue(Bytes.toBytes("0"), Bytes.toBytes("city"))),
      Bytes.toString(result._2.getValue(Bytes.toBytes("0"), Bytes.toBytes("channel")))
    )).toDF("rowkey", "date", "country", "province", "city", "channel")
    //将hbase数据查询后，注册为临时表
    hbaseTable.registerTempTable("user_status_table")
    //测试查看表数据信息，show默认显示10条
    //    sqlContext.sql("select * from user_status_table").show()
  }

  /**
    * Author: xinghua
    * param: hdfs数据与hbase数据关联，将结果插入hive中
    * Date： 2018/8/4 14:16 .registerTempTable("user_count")
    **/
  def dataToHive() = {
    sqlContext.sql("select ust.date date,ust.country country,ust.province province," +
      "u.uid user_id,u.pageid pageid,u.actionid  actionid from user u  " +
      "join user_status_table ust on u.uid=ust.rowkey" ).registerTempTable("user_count")
  }

  /**
    * Author: xinghua
    * param: 将数据进行统计整合，并存储至mysql中
    * Date： 2018/8/4 3:18
    **/
  def dataToMysql(): Unit = {
      sqlContext.sql("select date,country,province," +
        "Row_Number() OVER (PARTITION BY date,province,user_id order by date DESC ) AS user_count," +
        "pageid,actionid " +
        "from user_count").write.format("jdbc").mode(SaveMode.Append).
        jdbc(UserPropertis.getKeyValue("mysqlJdbc"),UserPropertis.getKeyValue("mysqlTable"),new Properties())
  }
}
