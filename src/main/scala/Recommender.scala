import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

class Recommender(model: MatrixFactorizationModel) {
  def recommend(user: Int, product: Int) = {
    try {
      model
        .recommendProducts(user, product)
        .map(rating => Recommendation(rating.product.toString, rating.rating))
        .toList
    } catch {
      case e: Exception => List.empty
    }
  }
}
