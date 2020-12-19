import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{CountVectorizer, NGram, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions.{asc, count, desc}

object TextProcessing extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  val sales = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/by-day/*.csv") //we took multiple csv files and loaded them together into a single DF
    .coalesce(5)
    .where("Description IS NOT NULL")
//    .sort("Description","Quantity")



  sales.show(10, truncate = false)

  val tkn = new Tokenizer().setInputCol("Description").setOutputCol("DescOut")
  val tokenized = tkn.transform(sales.select("Description"))
  tokenized.printSchema()
  tokenized.show(false)

  import org.apache.spark.ml.feature.RegexTokenizer
  val rt = new RegexTokenizer()
    .setInputCol("Description")
    .setOutputCol("DescOut")
//    .setPattern("T") // simplest expression probably will not need to split by T though
    .setPattern(" ") // simplest expression probably will not need to split by T though
    .setToLowercase(false)
  rt.transform(sales.select("Description")).show(false)

  //Another way of using the RegexTokenizer is to use it to output values matching the provided
  //pattern instead of using it as a gap. We do this by setting the gaps parameter to false. Doing this
  //with a space as a pattern returns all the spaces, which is not too useful, but if we made our
  //pattern capture individual words, we could return those
  import org.apache.spark.ml.feature.RegexTokenizer
  val wordTokenizer = new RegexTokenizer()
    .setInputCol("Description")
    .setOutputCol("DescOut")
    .setPattern("\\w+") //we need to double escape \ because it is used in regex
//    .setPattern("\\w*\\d+\\w*") //we need to double escape \ because it is used in regex
    .setGaps(false) //by using false we make the regex capture the patterns
    .setToLowercase(true)

  val tokenized2 = wordTokenizer.transform(sales.select("Description"))
  tokenized2.show(truncate = false)

  //A common task after tokenization is to filter stop words, common words that are not relevant in
  //many kinds of analysis and should thus be removed. Frequently occurring stop words in English
  //include “the,” “and,” and “but.” Spark contains a list of default stop words you can see by calling
  //the following method, which can be made case insensitive if necessary (as of Spark 2.2,
  //supported languages for stopwords are “danish,” “dutch,” “english,” “finnish,” “french,”
  //“german,” “hungarian,” “italian,” “norwegian,” “portuguese,” “russian,” “spanish,” “swedish,”
  //and “turkish”):
  val englishStopWords = StopWordsRemover
    .loadDefaultStopWords("english")

  //TODO val myStopWords = add two words to englishStopWords Array
  val updatedStopWords = englishStopWords ++ Array("rabbit", "blue")

  val stops = new StopWordsRemover()
    .setStopWords(updatedStopWords)
    .setInputCol("DescOut")
    .setOutputCol("NoStopWords")
  val removedWords = stops.transform(tokenized2)
  removedWords.show(false)
  //Tokenizing our strings and filtering stop words leaves us with a clean set of words to use as
  //features. It is often of interest to look at combinations of words, usually by looking at colocated
  //words. Word combinations are technically referred to as n-grams—that is, sequences of words of
  //length n. An n-gram of length 1 is called a unigrams; those of length 2 are called bigrams, and
  //those of length 3 are called trigrams (anything above those are just four-gram, five-gram, etc.),
  //Order matters with n-gram creation, so converting a sentence with three words into bigram
  //representation would result in two bigrams. The goal when creating n-grams is to better capture
  //sentence structure and more information than can be gleaned by simply looking at all words
  //individually
  val unigram = new NGram().setInputCol("DescOut").setN(1)
  val bigram = new NGram().setInputCol("DescOut").setN(2)
  val trigram = new NGram().setInputCol("DescOut").setN(3)
  val fourgram = new NGram().setInputCol("DescOut").setN(4)

  unigram.transform(tokenized.select("DescOut")).show(false)
  bigram.transform(tokenized.select("DescOut")).show(false)
  trigram.transform(tokenized2.select(col = "DescOut")).show(false)
  fourgram.transform(tokenized2.select(col = "DescOut")).show(false)


  //so we can use it on a larger dataframe it will use "DescOut" because we said so in setInputCol !
  bigram.transform(tokenized2).show(false)
  //we can change column on the fly
  bigram
    .setInputCol("NoStopWords")
    .transform(removedWords)
    .show(false)

  //Once you have word features, it’s time to start counting instances of words and word
  //combinations for use in our models. The simplest way is just to include binary counts of a word
  //in a given document (in our case, a row). Essentially, we’re measuring whether or not each row
  //contains a given word. This is a simple way to normalize for document sizes and occurrence
  //counts and get numerical features that allow us to classify documents based on content.
  val cv = new CountVectorizer()
    .setInputCol("DescOut")
    .setOutputCol("countVec")
    .setVocabSize(1500)
    .setMinTF(1) //the word should at least once
    .setMinDF(2) //the word should appear in at least two rows
  val fittedCV = cv.fit(tokenized)
  val countVectorDF = fittedCV.transform(tokenized)
  countVectorDF.printSchema()
  countVectorDF.show(false)

  //Term frequency–inverse document frequency
  //Another way to approach the problem of converting text into a numerical representation is to use
  //term frequency–inverse document frequency (TF–IDF). In simplest terms, TF–IDF measures
  //how often a word occurs in each document, weighted according to how many documents that
  //word occurs in. The result is that words that occur in a few documents are given more weight
  //than words that occur in many documents. In practice, a word like “the” would be weighted very
  //low because of its prevalence while a more specialized word like “streaming” would occur in
  //fewer documents and thus would be weighted higher.
  // in Scala
  val tfIdfIn = tokenized
    .where("array_contains(DescOut, 'red')")
    .select("DescOut")
    .limit(10)
  tfIdfIn.show(false)

  //let’s input that into TF–IDF. To do this, we’re going to hash each
  //word and convert it to a numerical representation, and then weigh each word in the voculary
  //according to the inverse document frequency. Hashing is a similar process as CountVectorizer,
  //but is irreversible—that is, from our output index for a word, we cannot get our input word
  //(multiple words might map to the same output index):
  // in Scala
  import org.apache.spark.ml.feature.{HashingTF, IDF}
  val tf = new HashingTF()
    .setInputCol("DescOut")
    .setOutputCol("TFOut")
    .setNumFeatures(10000)
  val idf = new IDF()
    .setInputCol("TFOut")
    .setOutputCol("IDFOut")
    .setMinDocFreq(2)

  // in Scala
  idf.fit(tf.transform(tfIdfIn)).transform(tf.transform(tfIdfIn)).show(false)


  //Word2Vec is a deep learning–based tool for computing a vector representation of a set of words.
  //The goal is to have similar words close to one another in this vector space, so we can then make
  //generalizations about the words themselves. This model is easy to train and use, and has been
  //shown to be useful in a number of natural language processing applications, including entity
  //recognition, disambiguation, parsing, tagging, and machine translation.
  //Word2Vec is notable for capturing relationships between words based on their semantics. For
  //example, if v~king, v~queen, v~man, and v~women represent the vectors for those four words,
  //then we will often get a representation where v~king − v~man + v~woman ~= v~queen. To do
  //this, Word2Vec uses a technique called “skip-grams” to convert a sentence of words into a
  //vector representation (optionally of a specific size). It does this by building a vocabulary, and
  //then for every sentence, it removes a token and trains the model to predict the missing token in
  //the "n-gram” representation. Word2Vec works best with continuous, free-form text in the form
  //of tokens.
  import org.apache.spark.ml.feature.Word2Vec
  import org.apache.spark.ml.linalg.Vector
  import org.apache.spark.sql.Row
  // Input data: Each row is a bag of words from a sentence or document.
  val documentDF = spark.createDataFrame(Seq(
    "Hi I heard about Spark".split(" "),
    "I wish Java could use case classes".split(" "),
    "Logistic regression models are neat".split(" ")
  ).map(Tuple1.apply)).toDF("text")
  // Learn a mapping from words to Vectors.
  val word2Vec = new Word2Vec()
    .setInputCol("text")
    .setOutputCol("result")
    .setVectorSize(3)
    .setMinCount(0)
  val model = word2Vec.fit(documentDF)
  val result = model.transform(documentDF)
  result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
  }

}
