CS143 Lab 1 (Scala)
 databricks-logo
databricks
Home
Workspace
Recent
Tables
Clusters
Jobs
Search
 Detached  File  View: Code  PermissionsWorkspace Access Control is only available in Databricks Professional and Enterprise accounts. Upgrade now Run All Clear Results PublishThis notebook will be published publicly and anyone with the link can view it Comments Revision history

Word Count Lab: Building a word count application
This lab will build on the techniques covered in the Spark tutorial to develop a simple word count application. The volume of unstructured text in existence is growing dramatically, and Spark is an excellent tool for analyzing this type of data. In this lab, we will write code that calculates the most common words in the Complete Works of William Shakespeare retrieved from Project Gutenberg. This could also be scaled to find the most common words on the Internet.
During this lab we will cover:
Part 1: Creating a base RDD and pair RDDs
Part 2: Counting with pair RDDs
Part 3: Finding unique words and a mean value
Part 4: Apply word count to a file
Before starting this lab you should familiarize yourself with the Databricks platform through the following Tutorial

Part 1: Creating a base RDD and pair RDDs
In this part of the lab, we will explore creating a base RDD with parallelize and using pair RDDs to count words.

(1a) Create a base RDD using the sc.parallelize method.
> 
val wordList = Array("cat", "elephant", "rat", "rat", "cat")
val wordsRDD = sc.parallelize(wordList)

// Print out the contents of the RDD
wordsRDD.collect.foreach(w => println(w))
(1) Spark Jobs
cat
elephant
rat
rat
cat
wordList: Array[String] = Array(cat, elephant, rat, rat, cat)
wordsRDD: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[3935] at parallelize at <console>:47
Command took 0.20 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:04:33 AM on My Cluster

(1b) Pluralize and test
Let's use a map() transformation to add the letter 's' to each string in the base RDD we just created. We'll define a Scala function that returns the word with an 's' at the end of the word. Please replace <FILL IN> with your solution. If you have trouble, the next cell has the solution. After you have defined makePlural you can run the third cell which contains a test. If you implementation is correct it will print 1 test passed.
This is the general form that exercises will take, except that no example solution will be provided. Exercises will include an explanation of what is expected, followed by code cells where one cell will have one or more <FILL IN> sections. The cell that needs to be modified will have TODO: Replace <FILL IN> with appropriate code on its first line. Once the <FILL IN> sections are updated and the code is run, the test cell can then be run to verify the correctness of your solution. The last code cell before the next markdown section will contain the tests.
> 
// TODO: Replace <FILL IN> with appropriate code

 /* Adds an 's' to `word`.
    Note:
        This is a simple function that only adds an 's'.  No attempt is made to follow proper
        pluralization rules.
    Args:
        word (str): A string.
    Returns:
        str: A string with 's' added to it.
*/
def makePlural(word: String): String = {
    var (addS: String) = "" // addS should be treated as String!
    addS = word + "s"
    return addS
} // if you want return, you need to write "{}"

println(makePlural("cat"))
cats
makePlural: (word: String)String
Command took 0.08 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:04:54 AM on My Cluster

Solution code hidden. Use cell drop down menu to unhide.
> 
// One possible solution
def makePlural(word: String) = word + "s"

println(makePlural("cat"))
cats
makePlural: (word: String)String
Command took 0.07 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:04:58 AM on My Cluster

(1c) Apply `makePlural` to the base RDD
Now pass each item in the base RDD into a map() transformation that applies the makePlural() function to each element. And then call the collect() action to see the transformed RDD.
> 
// TODO: Replace <FILL IN> with appropriate code
val pluralRDD = wordsRDD.map(makePlural)
pluralRDD.collect.foreach(println)
(1) Spark Jobs
cats
elephants
rats
rats
cats
pluralRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[572] at map at <console>:41
Command took 0.18 seconds -- by dui@g.ucla.edu at 2/7/2017, 1:04:33 PM on My Cluster
> 
// TEST Apply makePlural to base RDD(1c)

assert(pluralRDD.collect() sameElements Array("cats", "elephants", "rats", "rats", "cats"),
                  "incorrect values for pluralRDD")
(1) Spark Jobs
Command took 0.24 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:23 AM on My Cluster

(1d) Pass a closure (lambda) function to map.
Let's create the same RDD using a closure function.
> 
// TODO: Replace <FILL IN> with appropriate code
val pluralLambdaRDD = wordsRDD.map( (word:String) => word + "s" ) // this is a lambda function, anonymous function.

pluralLambdaRDD.collect.foreach(println)
<console>:39: error: not found: value word
              val pluralLambdaRDD = wordsRDD.map( word + "s" ) // this is a lambda function, anonymous function.
                                                  ^
Command took 0.04 seconds -- by dui@g.ucla.edu at 2/7/2017, 1:49:36 PM on My Cluster
> 
// TEST Pass a lambda function to map (1d)

assert(pluralLambdaRDD.collect() sameElements Array("cats", "elephants", "rats", "rats", "cats"),
                  "incorrect values for pluralRDD")
(1) Spark Jobs
Command took 0.21 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:27 AM on My Cluster

(1e) Length of each word
Now use map() and a lambda function to return the number of characters in each word. We'll collect this result directly into a variable.
> 
// TODO: Replace <FILL IN> with appropriate code
val pluralLengths = pluralRDD.map((word: String) => word.length()).collect()
pluralLengths.foreach(print)
(1) Spark Jobs
49444pluralLengths: Array[Int] = Array(4, 9, 4, 4, 4)
Command took 0.14 seconds -- by dui@g.ucla.edu at 2/7/2017, 1:50:05 PM on My Cluster
> 
// Test Length of each word (1e)

assert(pluralLengths sameElements Array(4, 9, 4, 4, 4),
                  "incorrect values for pluralLengths")
Command took 0.20 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:31 AM on My Cluster

(1f) Pair RDDs
The next step in writing our word counting program is to create a new type of RDD, called a pair RDD. A pair RDD is an RDD where each element is a pair tuple (k, v) where k is the key and v is the value. In this example we will create a pair consisting of (<word>, 1) for each word element in the RDD.
We can create the pair RDD using the map() transformation with a lambda function to create a new RDD.
> 
// TODO: Replace <FILL IN> with appropriate code

val wordPairs = wordsRDD.map( (word:String) => (word, 1)) // define the String named word!
wordPairs.collect.foreach(println)
(1) Spark Jobs
(cat,1)
(elephant,1)
(rat,1)
(rat,1)
(cat,1)
wordPairs: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[1089] at map at <console>:40
Command took 0.15 seconds -- by dui@g.ucla.edu at 2/7/2017, 2:33:00 PM on My Cluster
> 
// Test Pair RDDs (1f)

assert(wordPairs.collect sameElements Array(("cat", 1), ("elephant", 1), ("rat", 1), ("rat", 1), ("cat", 1)),
                  "incorrect value for wordPairs")
(1) Spark Jobs
Command took 0.20 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:35 AM on My Cluster

Part 2: Counting with pair RDDs
Now, let's count the number of times a particular word appears in the RDD. There are multiple ways to perform the counting, but some are much less efficient than others.
A naive approach would be to collect() all of the elements and count them in the driver program. While this approach could work for small datasets, we want an approach that will work for any size dataset including terabyte- or petabyte-sized datasets. In addition, performing all of the work in the driver program is slower than performing it in parallel in the workers. For these reasons, we will use data parallel operations.

(2a) GroupByKey approach
An approach you might first consider (we'll see shortly that there are better ways) is based on using the groupByKey() transformation. As the name implies, the groupByKey() transformation groups all the elements of the RDD with the same key into a single list in one of the partitions. There are two problems with using groupByKey():
The operation requires a lot of data movement to move all the values into the appropriate partitions.
The lists can be very large. Consider a word count of English Wikipedia: the lists for common words (e.g., the, a, etc.) would be huge and could exhaust the available memory in a worker.
Use groupByKey() to generate a pair RDD of type ('word', iterator).
> 
// TODO: Replace <FILL IN> with appropriate code
// Note that groupByKey requires no parameters

val wordsGrouped = wordPairs.groupByKey()
for { record <- wordsGrouped.collect
  key = record._1
  value = record._2} println(s"${key}: ${value}")

(1) Spark Jobs
elephant: CompactBuffer(1)
rat: CompactBuffer(1, 1)
cat: CompactBuffer(1, 1)
wordsGrouped: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[2363] at groupByKey at <console>:43
Command took 0.30 seconds -- by dui@g.ucla.edu at 2/7/2017, 8:38:44 PM on My Cluster
> 
// Test groupByKey() approach (2a)

assert(wordsGrouped.collect.map(r => (r._1, r._2.toList)).sortBy(_._1) 
       sameElements Array(("cat", List(1, 1)), ("elephant", List(1)), ("rat", List(1, 1))),
                  "incorrect value for wordsGrouped")
(1) Spark Jobs
Command took 0.33 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:43 AM on My Cluster

(2b) Use groupByKey to obtain the counts
> 
Using the `groupByKey()` transformation creates an RDD containing 3 elements, each of which is a pair of a word and an iterator.

Now sum the iterator using a `map()` transformation. The result should be a pair RDD consisting of `(word, count)` pairs.
> 
// TODO: Replace <FILL IN> with appropriate code
// Note: you can sum a Sequence of values using the Scala 'sum' method

val wordCountsGrouped = wordsGrouped.map( word => (word._1,(word._2).sum) )

wordCountsGrouped.collect.foreach(group => println(group))
(1) Spark Jobs
(elephant,1)
(rat,2)
(cat,2)
wordCountsGrouped: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[2079] at map at <console>:45
Command took 0.22 seconds -- by dui@g.ucla.edu at 2/7/2017, 5:47:33 PM on My Cluster
> 
// Test Use groupByKey to obtain the counts (2b)

assert(wordCountsGrouped.collect.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCountsGrouped")
(1) Spark Jobs
Command took 0.27 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:47 AM on My Cluster

(2c) Counting using reduceByKey
A better approach is to start from the pair RDD and then use the reduceByKey() transformation to create a new pair RDD. The reduceByKey() transformation gathers together pairs that have the same key and applies the function provided to two values at a time, iteratively reducing all of the values to a single value. reduceByKey() operates by applying the function first within each partition on a per-key basis and then across the partitions, allowing it to scale efficiently to large datasets.
> 
 // TODO: Replace <FILL IN> with appropriate code
// Note that reduceByKey takes in a function that accepts two values and returns a single value
val wordCounts = wordPairs.reduceByKey(_+_)
wordCounts.collect.foreach(println _)
(1) Spark Jobs
(elephant,1)
(rat,2)
(cat,2)
wordCounts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[2268] at reduceByKey at <console>:40
Command took 0.48 seconds -- by dui@g.ucla.edu at 2/7/2017, 8:24:43 PM on My Cluster
> 
// Test Use reduceByKey to obtain the counts (2c)

assert(wordCounts.collect.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCounts")
(1) Spark Jobs
Command took 0.24 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:51 AM on My Cluster

(2d) All together
> 
The expert version of the code performs `map()` to pair RDD, `reduceByKey()` transformation, and collect in one statement.
> 
// TODO: Replace <FILL IN> with appropriate code

val wordCountsCollected = wordsRDD.map((word:String) => (word, 1)).reduceByKey(_+_).collect()
wordCountsCollected.foreach(println)
(1) Spark Jobs
(elephant,1)
(rat,2)
(cat,2)
wordCountsCollected: Array[(String, Int)] = Array((elephant,1), (rat,2), (cat,2))
Command took 0.28 seconds -- by dui@g.ucla.edu at 2/7/2017, 8:40:40 PM on My Cluster
> 
// Test All together (2d)

assert(wordCountsCollected.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCounts")
Command took 0.21 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:54 AM on My Cluster

Part 3: Finding unique words and a mean value
(3a) Unique words
Calcuate the number of unique words in wordsRDD. You can use other RDDs that you have already created to make this easier.
> 
// TODO: Replace <FILL IN> with appropriate code
val uniqueWords = wordsRDD.distinct().count() // distinct().count()
println(s"Unique words = ${uniqueWords}")
(1) Spark Jobs
Unique words = 3
uniqueWords: Long = 3
Command took 0.14 seconds -- by dui@g.ucla.edu at 2/7/2017, 8:47:35 PM on My Cluster
> 
// Test unique words (3a)
assert(uniqueWords == 3, "incorrect count of unique words")
Command took 0.09 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:05:57 AM on My Cluster

(3b) Mean using reduce
Find the mean number of words per unique word in wordCounts. Use a reduce() action to sum the counts in wordCounts and then divide by the number of unique words. First map() the pair RDD wordCounts, which consists of (key, value) pairs, to an RDD of values.
> 
// TODO: Replace <FILL IN> with appropriate code

val totalCount = wordCounts.map(r => r._2).reduce(_+_) // why _+_???
val average = totalCount / uniqueWords.toFloat
println(s"total count = ${totalCount}, average = ${average}")
<console>:58: error: not found: value r
              val totalCount = wordCounts.map(r._2).reduce(_+_) // why _+_
                                              ^
Command took 0.06 seconds -- by dui@g.ucla.edu at 2/8/2017, 12:54:54 AM on My Cluster
> 
// Test mean using reduce
def truncate(value: Float) = (math floor value * 100) / 100

assert (truncate(average) == 1.66, "incorrect value of average")
truncate: (value: Float)Double
Command took 0.10 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:00 AM on My Cluster

Part 4: Apply word count to a file
In this section we will finish developing our word count application. We'll have to build the wordCount function, deal with real world problems like capitalization and punctuation, load in our data source, and compute the word count on the new data.

(4a) Word count function
First, define a function for word counting. You should reuse the techniques that have been covered in earlier parts of this lab. This function should take in an RDD that is a list of words like wordsRDD and return a pair RDD that has all of the words and their associated counts.
> 
// TODO: Replace <FILL IN> with appropriate code

import org.apache.spark.rdd.RDD

/* Creates a pair RDD with word counts from an RDD of words.
    Args:
        wordListRDD (RDD of str): An RDD consisting of words.
    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
*/
def wordCount(wordListRDD: RDD[String]): RDD[(String, Int)] = {
    val wordPairs = wordListRDD.map( (word:String) => (word, 1))
    val wordCounts = wordPairs.reduceByKey(_+_)
    return wordCounts
}

wordCount(wordsRDD).collect().foreach(println)

(1) Spark Jobs
(elephant,1)
(rat,2)
(cat,2)
import org.apache.spark.rdd.RDD
wordCount: (wordListRDD: org.apache.spark.rdd.RDD[String])org.apache.spark.rdd.RDD[(String, Int)]
Command took 0.28 seconds -- by dui@g.ucla.edu at 2/8/2017, 12:53:37 AM on My Cluster
> 
// Test wordCount function (4a)

assert(wordCount(wordsRDD).collect.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCounts")
(1) Spark Jobs
Command took 0.29 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:05 AM on My Cluster

(4b) Capitalization and punctuation
Real world files are more complicated than the data we have been using in this lab. Some of the issues we have to address are:
Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
All punctuation should be removed.
Any leading or trailing spaces on a line should be removed.
Define the function removePunctuation that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces.
> 
// TODO: Replace <FILL IN> with appropriate code

// You may find this method helpful
def extractSpacesLettersNumbersIn(text: String) = {
  "[ a-zA-Z0-9]+".r findAllIn text
}

/* Removes punctuation, changes to lower case, and strips leading and trailing spaces.
    Note: Only spaces, letters, and numbers should be retained.  Other characters should be
          eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
          punctuation is removed.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
*/
def removePunctuation(text: String): String = {
    val a: String = extractSpacesLettersNumbersIn(text).mkString("");  
    val b: String = ("[a-zA-Z0-9]+".r findAllIn a).mkString(" ").toLowerCase();
    return b;   

}

println(removePunctuation("Hi, you!"))
println(removePunctuation(" No under_score!"))
hi you
no underscore
extractSpacesLettersNumbersIn: (text: String)scala.util.matching.Regex.MatchIterator
removePunctuation: (text: String)String
Command took 0.13 seconds -- by dui@g.ucla.edu at 2/8/2017, 12:32:58 AM on My Cluster
> 
// Test Capilitalization and punctuation (4b)

assert(removePunctuation(" The Elephant's 4 cats. ") == "the elephants 4 cats",
                  "incorrect definition for removePunctuation function")
Command took 0.08 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:09 AM on My Cluster

(4c) Load a text file
For the next part of this lab, we will use the Complete Works of William Shakespeare from Project Gutenberg. To convert a text file into an RDD, we use the sc.textFile() method. We also apply the recently defined removePunctuation() function using a map() transformation to strip out the punctuation and change all text to lowercase. Since the file is large we use take(15), so that we only print 15 lines.
> 
// Just run this code

import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils

FileUtils.copyURLToFile(new URL("http://web.cs.ucla.edu/~tcondie/cs143data/pg100.txt"), new File("/tmp/shakespeare.txt"))
dbutils.fs.mv("file:/tmp/shakespeare.txt", "dbfs:/tmp/shakespeare.txt")

val shakespeareRDD = sc.textFile("/tmp/shakespeare.txt", 8).map(removePunctuation)
println(shakespeareRDD.zipWithIndex().map({
  case (l, num) => s"${num}: ${l}"
}).take(15).mkString("\n"))
(2) Spark Jobs
0: the project gutenberg ebook of the complete works of william shakespeare by
1: william shakespeare
2: 
3: this ebook is for the use of anyone anywhere at no cost and with
4: almost no restrictions whatsoever you may copy it give it away or
5: reuse it under the terms of the project gutenberg license included
6: with this ebook or online at wwwgutenbergorg
7: 
8: this is a copyrighted project gutenberg ebook details below
9: please follow the copyright guidelines in this file
10: 
11: title the complete works of william shakespeare
12: 
13: author william shakespeare
14: 
import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils
shakespeareRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3961] at map at <console>:61
Command took 2.16 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:18 AM on My Cluster

(4d) Words from lines
Before we can use the wordcount() function, we have to address two issues with the format of the RDD:
The first issue is that that we need to split each line by its spaces.
The second issue is we need to filter out empty lines.
Apply a transformation that will split each element of the RDD by its spaces. For each element of the RDD, you should apply Scala's string split() function. You might think that a map() transformation is the way to do this, but think about what the result of the split() function will be.
> 
// TODO: replace <FILL IN> with appropraite code

val shakespeareWordsRDD = shakespeareRDD.flatMap( r => r.split(" ") )
val shakespeareWordCount = shakespeareWordsRDD.count()

println(shakespeareWordsRDD.top(5).mkString("\n"))
println(s"shakespeareWordCount = ${shakespeareWordCount}")
(2) Spark Jobs
zwaggerd
zounds
zounds
zounds
zounds
shakespeareWordCount = 913387
shakespeareWordsRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3965] at flatMap at <console>:63
shakespeareWordCount: Long = 913387
Command took 1.82 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:24 AM on My Cluster
> 
assert(shakespeareWordsRDD.top(5).mkString(",") == "zwaggerd,zounds,zounds,zounds,zounds", "incorrect value for shakespeareWordsRDD")
(1) Spark Jobs
Command took 0.52 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:29 AM on My Cluster

(4e) Remove empty elements
The next step is to filter out the empty elements. Remove all entries where the word is "".
> 
// TODO: Replace <FILL IN> with appropriate code

val shakeWordsRDD = shakespeareWordsRDD.filter( r => r != "" ) // the condition is what we actually retain
val shakeWordCount = shakeWordsRDD.count()
println(s"shakeWordCount = ${shakeWordCount}")
(1) Spark Jobs
shakeWordCount = 903705
shakeWordsRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3969] at filter at <console>:68
shakeWordCount: Long = 903705
Command took 0.53 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:32 AM on My Cluster
> 
assert(shakeWordCount == 903705, "possible incorect value for shakeWordCount")
Command took 0.10 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:35 AM on My Cluster

(4f) Count the words
We now have an RDD that is only words. Next, let's apply the wordCount() function to produce a list of word counts. We can view the top 15 words by using the takeOrdered() action; however, since the elements of the RDD are pairs, we need a custom sort function that sorts using the value part of the pair.
You'll notice that many of the words are common English words. These are called stopwords. In a later lab, we will see how to eliminate them from the results.
Use the wordCount() function and takeOrdered() to obtain the fifteen most common words and their counts.
> 
// TODO: Replace <FILL IN> with appropraite code

val top15WordsAndCounts = wordCount(shakeWordsRDD).takeOrdered(15)(Ordering[Int].reverse.on(_._2)) // _._2, second value of pair, takeOrdered(n, [ordering])

println(top15WordsAndCounts.mkString("\n"))
(1) Spark Jobs
(the,27825)
(and,26791)
(i,20681)
(to,19261)
(of,18289)
(a,14667)
(you,13716)
(my,12481)
(that,11135)
(in,11027)
(is,9621)
(not,8745)
(for,8261)
(with,8046)
(me,7769)
top15WordsAndCounts: Array[(String, Int)] = Array((the,27825), (and,26791), (i,20681), (to,19261), (of,18289), (a,14667), (you,13716), (my,12481), (that,11135), (in,11027), (is,9621), (not,8745), (for,8261), (with,8046), (me,7769))
Command took 0.85 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:06:39 AM on My Cluster
> 
// Test top 15 words and counts

assert(top15WordsAndCounts.mkString(",") == "(the,27825),(and,26791),(i,20681),(to,19261),(of,18289),(a,14667),(you,13716),(my,12481),(that,11135),(in,11027),(is,9621),(not,8745),(for,8261),(with,8046),(me,7769)", "incorrect value for top15WordsAndCounts")
Command took 0.11 seconds -- by dui@g.ucla.edu at 2/8/2017, 1:09:38 AM on My Cluster
Shift+Enter to run    shortcuts
Send Feedback
