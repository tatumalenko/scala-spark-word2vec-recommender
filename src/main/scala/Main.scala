import extensions.Extensions._
import model.{MovieModel, MovieWrapper}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jfree.chart.{ChartFactory, ChartFrame}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import processing.MovieDataProcessor

object Main {
  var movies: RDD[MovieWrapper] = _

  var movieWrappers: Array[MovieWrapper] = _

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "train" => train()
      case "load" => loadOrTrain()
      case "run" => plot()
      case _ => println("No correct arguments supplied. Exiting now.")
    }
  }

  def train(): Array[MovieWrapper] = {
    val conf = new SparkConf().setAppName("Recommender").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkSession = SparkSession.builder.config(conf = conf).appName("Recommender").getOrCreate()
    val movieData = MovieDataProcessor.process(sparkSession, "src/main/resources/AllMoviesDetailsCleaned.csv")
    val model = MovieModel("word2vec.model")
    val trainedModel = model.train(movieData)
    model.save()
    val vectors = trainedModel.getVectors
    movies = movieData.rdd
      .map(movie => MovieWrapper(movie, vectors))
      .filter(movie => !movie.movie.overview.getOrElse("").isEmpty)
    // TODO: Use RowMatrix.columnSimilarities() to optimize computations
    // val rows = new RowMatrix(movies.map(arr => Vectors.dense(arr.vector.map(_.toDouble))).filter(_.size > 0))
    // val sims = rows.columnSimilarities(0.05)
    findSimilarities(movies.first())
  }

  def loadOrTrain(): Array[MovieWrapper] = {
    if (movieWrappers != null) movieWrappers else {
      try {
        movieWrappers = SerializableUtils.load[Array[MovieWrapper]]("movieWrappers.model")
        movieWrappers
      } catch {
        case x: Exception => train()
      }
    }
  }

  def findSimilarities(to: MovieWrapper): Array[MovieWrapper] = {
    val similarities = movies.map(movie => {
      movie.cosineSimilarity(to)
      movie
    })
    movieWrappers = similarities.collect()
    SerializableUtils.save(movieWrappers, "movieWrappers.model")
    movieWrappers
  }

  def plot(): Unit = {
    // TODO: Change this to plot the similarities with the genres on a XY-chart
    val dataset = new XYSeriesCollection()
    val series = new XYSeries("horror")
    List((1.0, 1.0), (2.0, 2.0)).foreach(point => series.add(point._1, point._2))
    dataset.addSeries(series)

    val chart = ChartFactory.createScatterPlot(
      "Scatter",
      "X",
      "Y",
      dataset
    )

    val frame = new ChartFrame("Scatter", chart)
    frame.pack()
    frame.setVisible(true)
  }
}
