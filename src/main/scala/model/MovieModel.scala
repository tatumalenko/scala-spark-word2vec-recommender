package model

import extensions.Extensions.{SerializableUtils, _}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.Dataset
import processing.MovieData

class MovieModel(path: String) {
  private val model: Word2Vec = new Word2Vec()
  private var trainedModel: Word2VecModel = _

  def train(movieData: Dataset[MovieData]): Word2VecModel = {
    val rddData = movieData.rdd.map(e => e.overview.getOrElse("").split(" ").toSeq)
    trainedModel = model.fit(rddData)
    trainedModel
  }

  def save(): Unit = {
    trainedModel.save(path)
  }

  def load(): Word2VecModel = {
    if (trainedModel != null) trainedModel else {
      trainedModel = SerializableUtils.load[Word2VecModel](path)
      trainedModel
    }
  }
}

object MovieModel {
  def apply(path: String) = new MovieModel(path)
}
