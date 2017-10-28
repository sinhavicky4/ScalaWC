import org.apache.spark.{SparkConf, SparkContext}

object CountWord {

  def main(args: Array[String]): Unit = {

    val inputPath:String =args(0)
    val outputpath:String=args(1);
    //getting spark configuration
    val conf=new SparkConf().setMaster("local").setAppName("wordcount");
    val sc=new SparkContext(conf);
    val rdd=sc.textFile(inputPath);
    val words=rdd.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey((arg,value)=>(arg+value)).saveAsTextFile(outputpath)

  }

}
