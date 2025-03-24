import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: WordCount <input-path> <output-path>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // Read input file as RDD
    val textFile = sc.textFile(inputPath)

    // Word count using RDD transformations
    val wordCounts = textFile
      .flatMap(line => line.split("\\W+"))  // Split words by non-word characters (regex)
      .filter(_.nonEmpty)                   // Remove empty words
      .map(word => (word.toLowerCase, 1))   // Convert to lowercase and map to (word, 1)
      .reduceByKey(_ + _)                    // Reduce by key to get word counts

    // Save as text file
    wordCounts.saveAsTextFile(outputPath)

    // Stop SparkContext
    sc.stop()
  }
}
