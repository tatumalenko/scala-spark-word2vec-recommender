package model

import extensions.Extensions._
import processing.MovieData

case class MovieWrapper(movie: MovieData, vectors: Map[String, Array[Float]]) {
  val vector: Array[Float] = movie.overview.getOrElse("")
    .split(" ")
    .foldLeft(Array[Float]())((vector, word) => vectors.getOrElse(word, Array[Float]()).average(vector))

  val norm: Double = vector.norm

  var similarity: Double = _

  def cosineSimilarity(other: MovieWrapper): Double = {
    similarity = vector.dot(other.vector) / (norm * other.norm)
    similarity
  }
}