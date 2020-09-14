package processing

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

case class MovieDataRaw(id: Option[Int],
                        budget: Option[Int],
                        genres: Option[String],
                        imdbId: Option[String],
                        originalLanguage: Option[String],
                        originalTitle: Option[String],
                        overview: Option[String],
                        popularity: Option[Float],
                        productionCompanies: Option[String],
                        productionCountries: Option[String],
                        releaseDate: Option[String],
                        revenue: Option[Int],
                        runtime: Option[Int],
                        spokenLanguages: Option[String],
                        status: Option[String],
                        tagline: Option[String],
                        title: Option[String],
                        voteAverage: Option[Float],
                        voteCount: Option[Int],
                        productionCompaniesNumber: Option[Int],
                        productionCountriesNumber: Option[Int],
                        spokenLanguagesNumber: Option[Int])

case class MovieData(id: Option[Int],
                     budget: Option[Int],
                     genres: List[String],
                     imdbId: Option[String],
                     originalLanguage: Option[String],
                     originalTitle: Option[String],
                     overview: Option[String],
                     popularity: Option[Float],
                     productionCompanies: Option[String],
                     productionCountries: Option[String],
                     releaseDate: Option[String],
                     revenue: Option[Int],
                     runtime: Option[Int],
                     spokenLanguages: Option[String],
                     status: Option[String],
                     tagline: Option[String],
                     title: Option[String],
                     voteAverage: Option[Float],
                     voteCount: Option[Int],
                     productionCompaniesNumber: Option[Int],
                     productionCountriesNumber: Option[Int],
                     spokenLanguagesNumber: Option[Int])

object MovieDataProcessor {
  def process(sparkSession: SparkSession, csvPath: String): Dataset[MovieData] = {
    import sparkSession.implicits._
    sparkSession
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(Encoders.product[MovieDataRaw].schema)
      .csv(csvPath)
      .as[MovieDataRaw]
      .map(raw => MovieData(
        raw.id,
        raw.budget,
        raw.genres.getOrElse("").split("\\|").toList,
        raw.imdbId,
        raw.originalLanguage,
        raw.originalTitle,
        raw.overview,
        raw.popularity,
        raw.productionCompanies,
        raw.productionCountries,
        raw.releaseDate,
        raw.revenue,
        raw.runtime,
        raw.spokenLanguages,
        raw.status,
        raw.tagline,
        raw.title,
        raw.voteAverage,
        raw.voteCount,
        raw.productionCompaniesNumber,
        raw.productionCountriesNumber,
        raw.spokenLanguagesNumber
      ))
  }
}
