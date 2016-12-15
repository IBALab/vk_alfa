

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.commons.lang3.StringUtils.getJaroWinklerDistance
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{ FileSystem, Path }

object scalaMatch {

  def main(args: Array[String]) {
    
    //removeLogs()
    checkArgs(args)
    
    val conf = new SparkConf().setAppName("find matches")
   //   .setMaster("local")
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)


    val pathToVkFiles = args(0)
    val pathToBankFile = args(1)
    val outputPath = args(2)

    //get vk users bdates
    var vkFileNames = FileSystem.get(new Configuration()).listStatus(new Path(pathToVkFiles)).map(x => x.getPath.getName)
    import org.apache.commons.io.FilenameUtils;
    vkFileNames = vkFileNames.map(x => FilenameUtils.removeExtension(x))
    val vkFileBdateRdd = sc.parallelize(vkFileNames)
    val vkFileBdateRddRow = vkFileBdateRdd.map(x => Row(x))

    //get  bank users
    val bankUserDf = getBankUserDf(pathToBankFile, sc, sqlContext)
    println("-->>get distinct bank users bdates")

    //get distinct bank users bdates
    val bankBdateRddRow = bankUserDf.map(x => Row(x.getAs("bank_correct_bdate")))
    val structBdate = StructType(
      StructField("bank_bdate", StringType, false) :: Nil)
    val bankBdateDf = sqlContext.createDataFrame(bankBdateRddRow, structBdate)
    val distinctBankBdateDf = bankBdateDf.dropDuplicates()

    var vkBdateDf = sqlContext.createDataFrame(vkFileBdateRddRow, structBdate)
    println("-->>intersect of bdates")
    //intersect of bdates
    val bdateIntersectionDf = distinctBankBdateDf.intersect(vkBdateDf).withColumnRenamed("bank_bdate", "bank_bdate_filtered")

    println("-->>filter bank users with bdates from intersection of bdates")
    //filter bank users with bdates from intersection of bdates
    val bankUserDfFiltered = bankUserDf.join(bdateIntersectionDf, bankUserDf("bank_correct_bdate") === bdateIntersectionDf("bank_bdate_filtered")).drop("bank_bdate_filtered")

    //dataframe with bank users 
    val vkUsers = sqlContext.read.json(pathToVkFiles)
    vkUsers.registerTempTable("vkUsers")
    val vkUserDfWithNull = sqlContext.sql("select id as vk_id , first_name as vk_first_name,last_name as vk_last_name,bdate as vk_bdate,country.title as vk_country,city.title as vk_city ,mobile_phone as vk_mobile_phone,last_seen.time as last_seen from vkUsers")
    val vkUserDf = vkUserDfWithNull.na.fill("")

    //dataframe with similarity 
    println("-->>dataframe with similarity ")
    val bankAndVkUsersWithSimilarityDfAll = getDfWithSimilarity(bankUserDfFiltered, vkUserDf, sqlContext)

    println("-->>sort and filter")
    //sort and filter
    //println("bank users : " + bankUserDfFiltered.count)

    val matchOutput = filterMatch(bankAndVkUsersWithSimilarityDfAll, 0.7).drop("bank_correct_bdate")
    //println("matchOutput " + matchOutput.count)

   /* matchOutput.coalesce(1)
   .write.format("com.databricks.spark.csv")
   .option("header", "false")
   .save(outputPath)*/
    var bestMatchString = matchOutput.map(x => x.mkString(",").replaceAll("[\\r\\n]", "") )
    bestMatchString.coalesce(1).saveAsTextFile(outputPath)

    //println(matchOutput.count +" vk users from " + bankUserDfFiltered.count + " bank users were found.")
    println("Successfully done!")

  }

  def filterMatch(bankAndVkUsersWithSimilarityDfAll: DataFrame, threshold: Double) = {
    var bankAndVkUsersWithHighSimilarity = bankAndVkUsersWithSimilarityDfAll.filter(bankAndVkUsersWithSimilarityDfAll("similarity") > threshold).orderBy(bankAndVkUsersWithSimilarityDfAll("similarity").desc)
    //println(bankAndVkUsersWithHighSimilarity.count)
    bankAndVkUsersWithHighSimilarity = bankAndVkUsersWithHighSimilarity
    //!!!!!!!!!!!!!!!!!!?????????????.distinct()
    //println(bankAndVkUsersWithHighSimilarity.count)
    var maxSimInGroup = bankAndVkUsersWithHighSimilarity.groupBy("bank_id").max("similarity")
    var namesForMaxSimInGroup = Seq("m_bank_id", "m_similarity")
    var maxSimInGroupRenamed = maxSimInGroup.toDF(namesForMaxSimInGroup: _*)
    var bestMatch = bankAndVkUsersWithHighSimilarity.join(maxSimInGroupRenamed, (maxSimInGroupRenamed("m_bank_id") === bankAndVkUsersWithHighSimilarity("bank_id")) && (maxSimInGroupRenamed("m_similarity") === bankAndVkUsersWithHighSimilarity("similarity")))
    bestMatch = bestMatch.drop("m_bank_id").drop("m_similarity")
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.rowNumber
    val w = Window.partitionBy(bestMatch("bank_id")).orderBy(bestMatch("last_seen").desc)
    bestMatch = bestMatch.select(bestMatch("bank_id"), bestMatch("bank_first_name"), bestMatch("bank_last_name"), bestMatch("bank_bdate"), bestMatch("bank_country"), bestMatch("bank_city"), bestMatch("bank_mobile_phone"), bestMatch("bank_home_phone"), bestMatch("bank_has_delay"), bestMatch("vk_id"), bestMatch("vk_first_name"), bestMatch("vk_last_name"), bestMatch("vk_bdate"), bestMatch("vk_country"), bestMatch("vk_city"), bestMatch("vk_mobile_phone"), bestMatch("similarity"), rowNumber.over(w).alias("rn"))
    bestMatch = bestMatch.filter(bestMatch("rn") === 1).drop("rn")
    bestMatch
  }

  def removeLogs() = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
  }

  def checkArgs(args: Array[String]) = {
    if (args.length != 3) {
      var logger = Logger.getLogger(this.getClass())
      logger.error("=> wrong parameters number")
      System.err.println("Usage: scalaMatch <path to files with vk users> <path to file with bank users> <output path>")
      System.exit(1)
    }
  }

  def getBankUserDf(pathToBankFile: String, sc: SparkContext, sqlContext: SQLContext) = {
    //dataframe with bank users 
    var bankdUserRddString = sc.textFile(pathToBankFile)
    val header = bankdUserRddString.first()
    bankdUserRddString = bankdUserRddString.filter { x => x != header }
    val bankdUserParsed = bankdUserRddString.map(line => parse(line))
    val bankUserRddRow = bankdUserParsed.map(v => Row(v: _*))
    val bankUserDfWithNull = makeBankUserDf(bankUserRddRow, sqlContext)
    var bankUserDf = bankUserDfWithNull.na.fill("")
    bankUserDf
  }

  def makeBankUserDf(bankUserRddRow: RDD[Row], sqlContext: SQLContext) = {
    val structBank = StructType(
      StructField("bank_id", StringType, false) ::
        StructField("bank_first_name", StringType, false) ::
        StructField("bank_last_name", StringType, false) ::
        StructField("bank_bdate", StringType, false) ::
        StructField("bank_country", StringType, true) ::
        StructField("bank_city", StringType, true) ::
        StructField("bank_mobile_phone", StringType, true) ::
        StructField("bank_home_phone", StringType, true) ::
        StructField("bank_has_delay", StringType, true) ::
        StructField("bank_correct_bdate", StringType, true) :: Nil)
    val bankUserDf = sqlContext.createDataFrame(bankUserRddRow, structBank)
    bankUserDf
  }

  def isInArray(x: String, arr: Array[String]) = {
    arr.contains(x)
  }
  def getDfWithSimilarity(bankUserDf: DataFrame, vkUserDf: DataFrame, sqlContext: SQLContext) = {

    val joinedVkAndBank = bankUserDf.join(vkUserDf, bankUserDf("bank_correct_bdate") === vkUserDf("vk_bdate"))
    val a = joinedVkAndBank.map { x => (Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9)), Row(x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17))) }
    var bankAndVkUsersWithSimilarityRdd = a.map(x => {
      var b = x._1
      var v = x._2
      val similarity = probMatchNew(b, v)
      (b.toSeq ++ v.toSeq :+ similarity)

    })

    val bankAndVkUsersWithSimilarityRddRow = bankAndVkUsersWithSimilarityRdd.map(v => Row(v: _*))
    val bankAndVkUsersWithSimilarityDf = sqlContext.createDataFrame(bankAndVkUsersWithSimilarityRddRow, StructType(bankUserDf.schema ++ vkUserDf.schema).add("similarity", DoubleType, false))
    bankAndVkUsersWithSimilarityDf

  }
  def probMatchNew(bu: Row, vku: Row): Double = {
    var bf_name = bu.get(1).toString().toLowerCase()
    bf_name = toCyrillic(bf_name)
    var vf_name = vku.get(1).toString().toLowerCase()
    vf_name = toCyrillic(vf_name)
    var bl_name = bu.get(2).toString().toLowerCase()
    bl_name = toCyrillic(bl_name)
    var vl_name = vku.get(2).toString().toLowerCase()
    vl_name = toCyrillic(vl_name)
    //var bm_name = bu.get("bank_maiden_name").toString().toLowerCase()
    //bm_name = toCyrillic(bm_name)
    //var vm_name = vku.get("vk_maiden_name").toString().toLowerCase()
    //vm_name = toCyrillic(vm_name)

    val jw_f_name = myCompare(bf_name, vf_name);
    val jw_l_name = myCompare(bl_name, vl_name);
    //val jw_bdate = getJaroWinklerDistance(bu.get("vk_bdate")
    //.toString(), vku.get("vk_bdate").toString());
    val b_bdate = bu.get(9).toString()
    val jw_bdate = boolToInt(b_bdate
      .equals(vku.get(3).toString()));
    //val jw_m_name = getJaroWinklerDistance(bm_name, vm_name);
    val country = boolToInt(bu.get(4).toString()
      .equalsIgnoreCase(vku.get(4).toString()));
    val city = boolToInt(bu.get(5).toString().equalsIgnoreCase(vku.get(5).toString()));
    val mobile = myCompare(
      removeCodeFroMobile(bu.get(6).toString()), removeCodeFroMobile(vku.get(6)
        .toString()));
    // val mobile = boolToInt(bu.get(6).toString()
    //.equals(vku.get("vk_mobile_phone").toString()));
    val prob = (0.3 * jw_f_name + 0.35 * jw_l_name + 0.05 * jw_bdate + /* 0.1 * jw_m_name +*/ 0.05 * country
      + 0.05 * city + 0.2 * mobile);
    return prob;
  }

  def probMatch(vku: Row, bu: Row): Double = {
    var bf_name = bu.getAs("bank_first_name").toString().toLowerCase()
    bf_name = toCyrillic(bf_name)
    var vf_name = vku.getAs("vk_first_name").toString().toLowerCase()
    vf_name = toCyrillic(vf_name)
    var bl_name = bu.getAs("bank_last_name").toString().toLowerCase()
    bl_name = toCyrillic(bl_name)
    var vl_name = vku.getAs("vk_last_name").toString().toLowerCase()
    vl_name = toCyrillic(vl_name)
    //var bm_name = bu.getAs("bank_maiden_name").toString().toLowerCase()
    //bm_name = toCyrillic(bm_name)
    //var vm_name = vku.getAs("vk_maiden_name").toString().toLowerCase()
    //vm_name = toCyrillic(vm_name)

    val jw_f_name = myCompare(bf_name, vf_name);
    val jw_l_name = myCompare(bl_name, vl_name);
    //val jw_bdate = getJaroWinklerDistance(bu.getAs("vk_bdate")
    //.toString(), vku.getAs("vk_bdate").toString());
    val b_bdate = remove0fromBdate(bu.getAs("bank_bdate").toString())
    val jw_bdate = boolToInt(b_bdate
      .equals(vku.getAs("vk_bdate").toString()));
    //val jw_m_name = getJaroWinklerDistance(bm_name, vm_name);
    val country = boolToInt(bu.getAs("bank_country").toString()
      .equalsIgnoreCase(vku.getAs("vk_country").toString()));
    val city = boolToInt(bu.getAs("bank_city").toString().equalsIgnoreCase(vku.getAs("vk_city").toString()));
    val mobile = myCompare(
      removeCodeFroMobile(bu.getAs("bank_mobile_phone").toString()), removeCodeFroMobile(vku.getAs("vk_mobile_phone")
        .toString()));
    // val mobile = boolToInt(bu.getAs("vk_mobile_phone").toString()
    //.equals(vku.getAs("vk_mobile_phone").toString()));
    val prob = (0.3 * jw_f_name + 0.35 * jw_l_name + 0.05 * jw_bdate + /* 0.1 * jw_m_name +*/ 0.05 * country
      + 0.05 * city + 0.2 * mobile);
    return prob;
  }
  def myCompare(s1: String, s2: String) = {

    val similarity = getJaroWinklerDistance(s1, s2)
    //println(s1 + " " + s2 + " " + similarity)
    if (similarity < 0.7)
      0
    else
      similarity
  }
  def removeCodeFroMobile(m: String) = {
    var s = ""
    if (m.startsWith("+37529"))
      s = m.substring(6)
    else if (m.startsWith("37529"))
      s = m.substring(5)
    else if (m.startsWith("8029"))
      s = m.substring(4)
    s
  }
  def remove0fromBdate(str: String) = {

    val s = str.substring(0, 10)
    val pieces = s.split('-')
    var day = pieces(2).trim()
    var month = pieces(1).trim()
    var year = pieces(0).trim()
    if (day.startsWith("0")) {
      day = day.substring(1)
    }
    if (month.startsWith("0"))
      month = month.substring(1)
    day + "." + month + "." + year
  }

  implicit def boolToInt(b: Boolean) = if (b) 1 else 0

  def toCyrillic(str: String): String = {
    var strToChange = str
    strToChange = strToChange.replaceAll("ya", "я")
    //println("dar1 " + strToChange)
    val pattern = new Regex("[A-Za-zА-Яа-я134690' ',]")
    strToChange = (pattern.findAllIn(strToChange)).mkString("")
    //println("dar2 " + strToChange)

    var str1 = strToChange.toLowerCase().flatMap(charToCyrillic)
    //println(str1)
    str1

  }

  def charToCyrillic(c: Char) = {
    //"ts",'c',"ch","sh","sch",'i','e',"ju","ja"
    //val abcLat = List(' ','a','b','v','g','d','e','e',"zh",'z','i','y','k','l','m','n','o','p','r','s','t','u','f','h',"ts","ch","sh","sch",'i','e',"ju","ja")
    // val abcCyr =   List("",'а','б','в','г','д','е','ё', 'ж','з','и',"ий",'к','л','м','н','о','п','р','с','т','у','ф','х', 'ц','ч', 'ш','щ','ы','э', 'ю','я')
    val abcLat = List(' ', '1', '3', '4', '6', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    val abcCyr = List("", 'л', 'з', 'ч', 'б', 'я', 'о', 'а', 'б', 'ц', 'д', 'е', 'ф', 'г', 'х', 'и', 'ж', 'к', 'л', 'м', 'н', 'о', 'п', 'р', 'с', 'т', 'у', 'в', 'в', "кс", "ий", "з")
    val index = abcLat.indexOf(c)
    if (index != -1)
      abcCyr(index).toString()
    else s"$c"

  }

  def parse(line0: String) = {
    val line = line0.substring(1, line0.length() - 1)
    val pieces = line.split(';')
    val id = pieces(0).trim()
    val first_name = pieces(2).trim()
    val last_name = pieces(1).trim()
    val bdate = pieces(6).trim()
    //val maiden_name = pieces(4).trim()
    val country = pieces(4).trim()
    val city = pieces(5).trim()
    val mobile_number = pieces(7).trim()
    val home_number = pieces(8).trim()
    val has_delay = pieces(9).trim()
    Seq[Any](id, first_name, last_name, bdate, country, city, mobile_number, home_number, has_delay, remove0fromBdate(bdate))
  }
}
