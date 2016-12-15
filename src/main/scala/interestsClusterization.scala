

import scala.collection.mutable
import org.apache.spark.mllib.clustering.{ LDA, DistributedLDAModel, LocalLDAModel }
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Range

object interestsClusterization {

  def main(args: Array[String]) {

    //removeLogs()
    checkArgs(args)
    val conf = new SparkConf().setAppName("interests clusterization")
    //.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val matched_file = args(0)
    val pathToVkFiles = args(1)
    val outputPath = args(2)

    val matched = sc.textFile(matched_file)
    val matched_vk_id_df = getDF(matched, sqlContext)

    val vkUsers = sqlContext.read.json(pathToVkFiles).na.fill("")
    var vk = vkUsers.join(matched_vk_id_df, vkUsers("id") === matched_vk_id_df("vk_id"))
    vk.persist()
    val vkIdInterests = get_id_interests_df(vk, sqlContext)

    //do smth with empty interests
    val vkId = vkIdInterests.select("id").map(_.get(0).toString.toLong)

    //println("all users: " + vk.count() + "\n users with not empty info: " + vkIdnotNullInterests.count())
    val vkIntParsed = vkIdInterests.map(line => (line.getAs("id").toString().toLong, parse(line.getAs("allInfo").toString())))
    val vkIntParsedDf = create_id_allInfo_df(vkIntParsed, sqlContext)
    vk.unpersist()
    vkIntParsed.persist()
    var id_interests_from_vocabulary = make_vocabulary(vkIntParsedDf, sqlContext, sc, outputPath)
    vkIntParsed.unpersist()
    val id_interests_df = create_id_allInfo_df(id_interests_from_vocabulary, sqlContext)
    val id_interests_string = id_interests_df.map(x => x.mkString(","))
    id_interests_string.coalesce(1).saveAsTextFile(outputPath + "/id_all_info")
    clusterization( outputPath, vkId, sc)
    println("SUCCEED")

  }
  def create_id_allInfo_df(id_interests_from_vocabulary: RDD[(Long, String)], sqlContext: SQLContext) = {
    val RddRow = id_interests_from_vocabulary.map(x => Row(x._1, x._2))
    val struct_id_allInfo = StructType(
      StructField("id", LongType, false) ::
        StructField("allInfo", StringType, false) :: Nil)
    sqlContext.createDataFrame(RddRow, struct_id_allInfo)

  }
  def clusterization(outputPath: String, allvkId: RDD[Long], sc: SparkContext) = {

    val id_all_info_string: RDD[String] = sc.textFile(outputPath + "/id_all_info/part-00000")
    //val corpus: RDD[String] = id_interests_df.map(x=>x.getAs("allInfo").toString()).persist()
    val id_info_rdd = id_all_info_string.map(x => {
      val arr = x.split(",")
      (arr(0).toLong, arr(1))
    })
    val corpus = id_info_rdd.map(x => x._2)
    val strArray = corpus.toArray()
    val id = id_info_rdd.map(x => x._1).toArray()

    // Split each document into a sequence of terms (words)
    val tokenized: RDD[Seq[String]] =
      corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 2).filter(_.forall(java.lang.Character.isLetter)))

    val termCounts: Array[(String, Long)] =
      tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    val vocabArray: Array[String] = termCounts.map(_._1)
    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map {
        case (tokens, id) =>
          val counts = new mutable.HashMap[Int, Double]()
          tokens.foreach { term =>
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
          (id, Vectors.sparse(vocab.size, counts.toSeq))
      }
    //documents.collect.foreach(println)
    // Set LDA parameters
    val numTopics = 5
    val lda = new LDA().setK(numTopics).setMaxIterations(100).setAlpha(1.1).setSeed(42)
    //.setBeta(1.1)
    //.setAlpha(1.1)

    val ldaModel = lda.run(documents)
    println(lda.getAlpha + " " + lda.getBeta)

    //val avgLogLikelihood = ldaModel.logLikelihood / documents.count()
    var avgLogLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].toLocal.logLikelihood(documents) / documents.count()
    println("avgLogLikelihood : " + avgLogLikelihood)

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics()
    var i = 1
    var topic_word_weight: List[(String, String, Int)] = List[(String, String, Int)]()
    topicIndices.foreach {
      case (terms, termWeights) =>
        println("TOPIC: " + i)
        terms.zip(termWeights).foreach {
          case (term, weight) =>
            println(s"${vocabArray(term.toInt)}\t$weight")
            topic_word_weight ::= ("Topic " + i, vocabArray(term.toInt), (weight * 100000).toInt)
        }
        println()
        i += 1
    }
    val topic_word_weight_rdd = sc.parallelize(topic_word_weight.reverse, 1).map(x => x._1 + "," + x._2 + "," + x._3)
    topic_word_weight_rdd.saveAsTextFile(outputPath + "/topic_word_weight")

    val localLDAModel: LocalLDAModel = ldaModel.asInstanceOf[DistributedLDAModel].toLocal
    val topicDistributions = localLDAModel.topicDistributions(documents).toArray()
    //topicDistributions.foreach(println)
    var id_topic: List[(Long, String)] = List[(Long, String)]()
    var clusters = scala.collection.mutable.Map[Integer, Integer]()
    for (i <- Range(0, corpus.count().toInt)) {

      val arr = topicDistributions(i)._2.toArray
      val cluster = arr.indexOf(arr.max) + 1
      println(id(i) + " , " + strArray(i) + " , " + cluster)
      id_topic ::= (id(i), "Topic " + cluster)
      if (clusters.contains(cluster)) {
        //println("contains")
        val v = clusters.getOrElse(cluster, 0)
        clusters.update(cluster, v.toString().toInt + 1)
      } else
        clusters.put(cluster, 1)

    }

    val emptyInfoVkId = allvkId.filter(x => !id.contains(x)).toArray
    for (e <- emptyInfoVkId) {
      id_topic ::= (e, "Topic 0")
    }

    println(id_topic.size)
    val id_topic_badClient_rdd = sc.parallelize(id_topic, 1).map(x => x._1 + "," + x._2)
    id_topic_badClient_rdd.saveAsTextFile(outputPath + "/id_topic")
    //for ((k, v) <- clusters) printf("key: %d, value: %d \n", k, v)

  }

  def make_vocabulary(vkIdnotNullInterests: DataFrame, sqlContext: SQLContext, sc: SparkContext, outputPath: String) = {
    var id_word_rdd = vkIdnotNullInterests.flatMap(x => {
      val id = x.getAs("id").toString().toLong
      var word_list = x.getAs("allInfo").toString().split(" ").toList
      val word_id = word_list.zip(Stream.continually(id))
      word_id
    })
    val struct_word_id = StructType(
      StructField("word", StringType, false) ::
        StructField("id", LongType, false) :: Nil)
    val id_word_rdd_row = id_word_rdd.map(x => Row(x._1, x._2))
    val id_word_data = sqlContext.createDataFrame(id_word_rdd_row, struct_word_id)

    val stemmed = new Stemmer()
      .setInputCol("word")
      .setOutputCol("stemmed")
      .setLanguage("Russian")
      .transform(id_word_data)

    val collectStemmedWords = stemmed.map(x => (x.getAs("stemmed").toString(), 1)).reduceByKey(_ + _)
    val filteredWords = collectStemmedWords.filter(x => x._2 >= 3 && x._1.length() >= 3).sortBy(-_._2)
    var wordsToKeep = filteredWords.map(_._1)
    wordsToKeep
    //write vocabulary
    wordsToKeep.coalesce(1).saveAsTextFile(outputPath + "/vocabulary")
    //wordsToKeep = sc.textFile(outputPath+"/Vocabulary")
    stemmed.drop("word")
    val wordsToKeepArray = wordsToKeep.toArray()
    val id_word_from_voc = stemmed.rdd.filter(x => (cont(x.getAs("stemmed").toString(), wordsToKeepArray))).map(x => (x.getAs("id").toString().toLong, x.getAs("stemmed").toString()))
    val id_all_info_from_vocabulary = id_word_from_voc.reduceByKey(_ + " " + _)
    id_all_info_from_vocabulary
  }
  def cont(s: String, a: Array[String]) = {
    a.contains(s)
  }
  /*def func(a: (Long, String), b: (Long, String)) = {
    (a._1, a._2+" "+b._2)
        
  }*/
  def getDF(matched: RDD[String], sqlContext: SQLContext) = {
    val matched_id_rdd_row = matched.map(line => get_vk_id(line)).map(x => Row(x))
    val struct_vk_id = StructType(
      StructField("vk_id", LongType, false) :: Nil)
    val matched_vk_id_df = sqlContext.createDataFrame(matched_id_rdd_row, struct_vk_id)
    matched_vk_id_df
  }
  def get_id_interests_df(vk: DataFrame, sqlContext: SQLContext) = {
    val vkInfo = vk.select("id", "about", "activities", "books", "games", "interests", "movies", "music", "quotes", "tv")

    val vkInterestsRDD = vkInfo.rdd.map(x => {
      var all:String = ""
      for (i <- Range(1, x.length - 1)) {
        val curr = x(i).toString
        if (!curr.isEmpty())
          all = all + "." + x(i)
      }
      (x.getLong(0), all)
    })
    val struct_id_allInfo = StructType(
      StructField("id", LongType, false) ::
        StructField("allInfo", StringType, false) :: Nil)
    val vkInterestsRDDRow = vkInterestsRDD.map(x => Row(x._1, x._2))
    val vkInterests = sqlContext.createDataFrame(vkInterestsRDDRow, struct_id_allInfo)
    vkInterests
  }

  def isInVocabulary(x: String, arr: Array[String]) = {
    arr.contains(x)
  }
  def funct(x: String, cl: Array[Long]) = {
    cl.contains(x.toLong)
  }

  def getCosineSimilarity(a: Array[Int], b: Array[Int]) = {
    var dotProd = 0;
    var sqA = 0;
    var sqB = 0;

    for (i <- 0 to a.length - 1) {
      dotProd += a(i) * b(i);
      sqA += a(i) * a(i);
      sqB += b(i) * b(i);
    }
    dotProd / (Math.sqrt(sqA) * Math.sqrt(sqB));
  }
  def stringToVec(str: String, arr: Array[String]) = {
    val words = str.split(" ")
    //println(words.mkString(" ").trim() + " " + words.length)
    val s = arr.size
    var vec = Array.fill(s) { 0 }
    for (x <- words) {
      val wordFound = arr.toList.indexOf(x)
      if (wordFound > 0)
        vec.update(wordFound, 1)
    }
    //print( vec.toList)
    vec

  }

  def get_vk_id(line: String) = {

    val pieces = line.split(',')
    val vk_id = pieces(9).trim().toLong
    vk_id

  }
  def parse(str: String) = {

    var p = str.replaceAll("""[\p{Punct}]""", " ")
    p = p.replaceAll("""[^А-Яа-я]""", " ")
    p.toLowerCase().trim()

  }

  def removeLogs() = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
  }
    def checkArgs(args: Array[String]) = {
    if (args.length != 3) {
      var logger = Logger.getLogger(this.getClass())
      logger.error("=> wrong parameters number")
      System.err.println("Usage: interestsClusterization <path to matched file> <path to files with vk users>  <output path>")
      System.exit(1)
    }
  }
}