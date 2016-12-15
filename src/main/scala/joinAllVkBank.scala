import org.apache.spark.sql.functions.col
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe

object joinAllVkBank {

  def main(args: Array[String]) {
    //removeLogs()
    checkArgs(args)
    
    val conf = new SparkConf().setAppName("join all data from vk and bank")//.setMaster("local")
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new SQLContext(sc)

    

    val matched_file = args(0)
    val path_to_vk_files = args(1)
    val path_to_vk_fields_description = args(2)
    val path_to_interests_clusterization =  args(3)
    val outputPath = args(4)

    val matched = sc.textFile(matched_file)
    val matched_vk_id_df = getDF(matched, sqlContext)

    
    val vkUsers = sqlContext.read.json(path_to_vk_files).na.fill("")
    var vk = vkUsers.join(matched_vk_id_df, vkUsers("id") === matched_vk_id_df("matched_vk_id"))
    vk.persist()
    vk = dropInterests(vk)
    vk.registerTempTable("vkAndBankUsers")
    val vkUserDfwithNull = sqlContext.sql("select bdate," +
      "city.id as city_id,city.title as city_title,country.id as country_id,country.title as country_title," +
      "faculty, faculty_name,first_name,followers_count,graduation," +
      "home_town,id,occupation.type as occupation_type,occupation.name as occupation_name,personal," +
      "relation,sex,university,university_name,bank_id,bank_first_name,bank_last_name," +
      "bank_bdate,bank_country,bank_city,bank_mobile_phone,bank_home_phone,bank_has_delay from vkAndBankUsers")
    var vkUserDf = vkUserDfwithNull.na.fill("")
    var vkUserDfwithPersonal= vkUserDf.withColumn("is_working (occupation)", callUDF(func_for_occupation, IntegerType, col("occupation_type")))
    vkUserDfwithPersonal=vkUserDfwithPersonal.withColumn("has_delay",  func_for_delay( col("bank_has_delay")))
    vkUserDfwithPersonal = modifyDf(vkUserDfwithPersonal,path_to_vk_fields_description,sqlContext,sc)
    val vkUserDfres=vkUserDfwithPersonal.drop("personal").drop("all_religion_name").drop("occupation_type").drop( "political_id").drop("people_main_id")
    .drop("life_main_id").drop("smoking_id").drop("alcohol_id").drop("alcohol_id").drop("relation_id").drop("sex_id").drop("bank_has_delay")
    val interestsClusterization= sc.textFile(path_to_interests_clusterization)
    val interestsClusterizationDf = getInterestsClusterizationDf(interestsClusterization,sqlContext)
  
   var vkUserDfresWithInterestsClusterization  = vkUserDfres.join(interestsClusterizationDf,vkUserDfres("id")===interestsClusterizationDf("inter_vk_id")).drop("inter_vk_id")
   //vkUserDfresWithInterestsClusterization = vkUserDfresWithInterestsClusterization.withColumn("new_city_title", func_drop_whitespaces(col("city_title"))).withColumnRenamed("city_title", "new_city_title")
   // vkUserDfresWithInterestsClusterization = vkUserDfresWithInterestsClusterization.withColumn("new_country_title", func_drop_whitespaces(col("country_title"))).withColumnRenamed("country_title", "new_country_title")
   vkUserDfresWithInterestsClusterization = vkUserDfresWithInterestsClusterization.withColumn("new_faculty_name", func_drop_whitespaces(col("faculty_name"))).drop("faculty_name").withColumnRenamed("new_faculty_name", "faculty_name")
   vkUserDfresWithInterestsClusterization = vkUserDfresWithInterestsClusterization.withColumn("new_home_town", func_drop_whitespaces(col("home_town"))).drop("home_town").withColumnRenamed("new_home_town", "home_town")
   vkUserDfresWithInterestsClusterization = vkUserDfresWithInterestsClusterization.withColumn("new_occupation_name", func_drop_whitespaces(col("occupation_name"))).drop("occupation_name").withColumnRenamed("new_occupation_name", "occupation_name")
   vkUserDfresWithInterestsClusterization = vkUserDfresWithInterestsClusterization.withColumn("new_university_name", func_drop_whitespaces(col("university_name"))).drop("university_name").withColumnRenamed("new_university_name", "university_name")
  
   vkUserDfresWithInterestsClusterization.coalesce(1)
   .write.format("com.databricks.spark.csv")
   .option("header", "true")
   .save(outputPath) 

  }
    def func_drop_whitespaces = udf ((s:String)=>{
    s.replaceAll("[\\r\\n]", "") 
  })
  def getInterestsClusterizationDf(rdd: RDD[String], sqlContext: SQLContext) = {
    val rdd_row = rdd.map(line =>{
    val pieces = line.split(',')
    val vk_id = pieces(0).trim().toString().toLong
    val cl_id = pieces(1).trim().toString().replace("Topic ", "").toInt
    Seq[Any](vk_id,cl_id)
    }).map(Row(_: _*))
    val struct_cl = StructType(
      StructField("inter_vk_id", LongType, false) ::
        StructField("cluster_id", IntegerType, false):: Nil)
    val matched_vk_id_df = sqlContext.createDataFrame(rdd_row, struct_cl)
    matched_vk_id_df
  }
  
  def func_for_delay = udf ((i:Int)=>{
    if (i!=0)
      1
    else i
  })
  
  def modifyDf(df : DataFrame,path_to_vk_fields_description:String,sqlContext:SQLContext,sc:SparkContext)={
    
    var vkUserDfwithPersonal = df.withColumn("political", func_for_personal("political")(col("personal")))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("people_main", func_for_personal("people_main")(col("personal")))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("life_main", func_for_personal("life_main")(col("personal")))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("smoking", func_for_personal("smoking")(col("personal")))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("alcohol", func_for_personal("alcohol")(col("personal")))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("all_religion_name", (func_for_religion_name(col("personal"))))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("religion", (func_for_religion(col("all_religion_name"))))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("religion_name", (clear_religion_name(col("religion"),col("all_religion_name"))))
    vkUserDfwithPersonal = vkUserDfwithPersonal.withColumn("relation", (func_for_religion(col("all_religion_name"))))
    vkUserDfwithPersonal=vkUserDfwithPersonal.na.fill(0,Seq("relation"))
    val political_df=makeDfFromFieldsDescription(path_to_vk_fields_description+"/political.txt","political",sqlContext,sc)
    val people_main_df = makeDfFromFieldsDescription(path_to_vk_fields_description + "/people_main.txt","people_main",sqlContext,sc)
    val life_main_df = makeDfFromFieldsDescription(path_to_vk_fields_description + "/life_main.txt","life_main",sqlContext,sc)
    val smoking_df = makeDfFromFieldsDescription(path_to_vk_fields_description + "/smoking.txt","smoking",sqlContext,sc)
    val alcohol_df = makeDfFromFieldsDescription(path_to_vk_fields_description + "/alcohol.txt","alcohol",sqlContext,sc)
    val relation_df = makeDfFromFieldsDescription(path_to_vk_fields_description + "/relation.txt","relation",sqlContext,sc)
    val sex_df = makeDfFromFieldsDescription(path_to_vk_fields_description + "/sex.txt","sex",sqlContext,sc)
    vkUserDfwithPersonal=vkUserDfwithPersonal.join(political_df, political_df("political_id")===vkUserDfwithPersonal("political"))
    vkUserDfwithPersonal=vkUserDfwithPersonal.join(people_main_df, people_main_df("people_main_id")===vkUserDfwithPersonal("people_main"))
    vkUserDfwithPersonal=vkUserDfwithPersonal.join(life_main_df, life_main_df("life_main_id")===vkUserDfwithPersonal("life_main"))
    vkUserDfwithPersonal=vkUserDfwithPersonal.join(smoking_df, smoking_df("smoking_id")===vkUserDfwithPersonal("smoking"))
    vkUserDfwithPersonal=vkUserDfwithPersonal.join(alcohol_df, alcohol_df("alcohol_id")===vkUserDfwithPersonal("alcohol"))
    vkUserDfwithPersonal=vkUserDfwithPersonal.join(relation_df, relation_df("relation_id")===vkUserDfwithPersonal("relation"))
    vkUserDfwithPersonal=vkUserDfwithPersonal.join(sex_df, sex_df("sex_id")===vkUserDfwithPersonal("sex"))
    vkUserDfwithPersonal  
  }
 
  def makeDfFromFieldsDescription(fileName:String,name:String,sqlContext:SQLContext,sc:SparkContext)={
     val rdd = sc.textFile(fileName)
    val rddRow = rdd.map(parseFieldsDescription).map(Row(_:_*))
    val struct_fields_description = StructType(
      StructField(name+"_id", IntegerType, false) ::
        StructField(name+"_name", StringType, false)  :: Nil)
    val df = sqlContext.createDataFrame(rddRow, struct_fields_description)
    //df.show()
    df
  }
  
  def parseFieldsDescription(line: String) = {

    val l  =line.replaceAll("\\p{C}","")
    val pieces = l.split("-")
   // println(pieces(0).trim().length())
    val id = pieces(0).toString().toInt
    val name = pieces(1).toString()
    Seq[Any](id, name)
  }

  def func_for_personal(s: String) = {
    udf((col: String) => {
      var sr = col.replaceAll("[\\[\\]]", "")
      if (sr.startsWith("{") && sr.endsWith("}"))
        sr = sr.substring(1, sr.length() - 1)
      //println(sr)
      val map = getMapFromPersonal(sr)
      val res = map.getOrElse(s, 0).toString().toInt
      //println(r)
      res
    })
  }
  def clear_religion_name = udf((id: Int,name:String) => {
    if (id==0)
      ""
    else
      name
  })

  def func_for_religion_name = udf((s: String) => {
    var sr = s.replaceAll("[\\[\\]]", "")
    if (sr.startsWith("{") && sr.endsWith("}"))
      sr = sr.substring(1, sr.length() - 1)
    //println(sr)
    val map = getMapFromPersonal(sr)
    val res = map.getOrElse("religion", 0).toString()
    //println(r)
    res
  })
  def func_for_religion = udf((s: String) => {
    //println(s)
    var res: Int = 0
    s match {
      case "Иудаизм" => res = 1
      case "Православие" => res = 2
      case "Католицизм" => res = 3
      case "Протестантизм" => res = 4
      case "Ислам" => res = 5
      case "Буддизм" => res = 6
      case "Конфуцианство" => res = 7
      case "Светский гуманизм" => res = 8
      case "Пастафарианство" => res = 9
      case _ => res = 0
    }
    //println(res)
    res
  })

  def getMapFromPersonal(s: String) = {
    val arr = s.split(",")
    var myMap = scala.collection.mutable.Map[String, String]()
    var i = 0
    for (i <- 0 until arr.size) {
      val item = arr(i).split(":")
      if (item.size == 2) {
        var k = item(0)
        if (k.startsWith("\"") && k.endsWith("\""))
          k = k.substring(1, k.length() - 1)
        var v = item(1)
        if (v.startsWith("\"") && v.endsWith("\""))
          v = v.substring(1, v.length() - 1)
        myMap.put(k, v)
      }
    }
    myMap
  }

  def func_for_occupation: (String => Int) =
    (s: String) => {
      if (s.equals("university") || s.equals("school") || s.equals("") || s.isEmpty())
        //"".toString()
        0
      //else "work".toString()
      else 1

    }

  def dropInterests(df: DataFrame) = {
    df.drop("about").drop("crop_photo").drop("activities").drop("books").drop("games").drop("interests").drop("movies").drop("music").drop("quotes").drop("tv")

  }
  def removeLogs() = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }
  def checkArgs(args: Array[String]) = {
    if (args.length != 5) {
      var logger = Logger.getLogger(this.getClass())
      logger.error("=> wrong parameters number")
      System.err.println("Usage: joinAllVkBank <path to matched file> <path to vk files> <path to files with vk description fields>  <path to interest clusterization file > <output-path>")
      System.exit(1)
    }
  }
  def getDF(matched: RDD[String], sqlContext: SQLContext) = {
    val matched_id_rdd_row = matched.map(line => parse(line)).map(Row(_: _*))
    val struct_vk_id = StructType(
      StructField("matched_vk_id", LongType, false) ::
        StructField("bank_id", LongType, false) ::
        StructField("bank_first_name", StringType, false) ::
        StructField("bank_last_name", StringType, false) ::
        StructField("bank_bdate", StringType, false) ::
        StructField("bank_country", StringType, true) ::
        StructField("bank_city", StringType, true) ::
        StructField("bank_mobile_phone", StringType, true) ::
        StructField("bank_home_phone", StringType, true) ::
        StructField("bank_has_delay", IntegerType, true) :: Nil)
    val matched_vk_id_df = sqlContext.createDataFrame(matched_id_rdd_row, struct_vk_id)
    matched_vk_id_df
  }
  def parse(line: String) = {
    val pieces = line.split(',')
    val bank_id = pieces(0).trim().toString().toLong
    val first_name = pieces(1).trim()
    val last_name = pieces(2).trim()
    val bdate = pieces(3).trim()
    //val maiden_name = pieces(4).trim()
    val country = pieces(4).trim()
    val city = pieces(5).trim()
    val mobile_number = pieces(6).trim()
    val home_number = pieces(7).trim()
    val has_delay = pieces(8).trim().toString().toInt
    val vk_id = pieces(9).trim().toString().toLong
    Seq[Any](vk_id, bank_id, first_name, last_name, bdate, country, city, mobile_number, home_number, has_delay)
  }
}