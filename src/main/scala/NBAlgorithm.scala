package org.example.textclassification

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable.ListBuffer
import scala.math._

/** Define parameters for Supervised Learning Model. We are
 * using a Naive Bayes classifier, which gives us only one
 * hyperparameter in this stage.
 */
case class NBAlgorithmParams(lambda: Double) extends Params

/** Define SupervisedAlgorithm class. */
class NBAlgorithm(
  val ap: NBAlgorithmParams
) extends P2LAlgorithm[PreparedData, NBModel, Query, PredictedResults] {

  /** Train your model. */
  def train(sc: SparkContext, pd: PreparedData): NBModel = {
    // Fit a Naive Bayes model using the prepared data.
    val nb: NaiveBayesModel = NaiveBayes.train(pd.transformedData, ap.lambda)

    new NBModel(
      tfIdf = pd.tfIdf,
      categoryMap = pd.categoryMap,
      nb = nb)
  }

  /** Prediction method for trained model. */
  def predict(model: NBModel, query: Query): PredictedResults = {
    model.predict(query.text)
  }
}

class NBModel(
  val tfIdf: TFIDFModel,
  val categoryMap: Map[Double, String],
  val nb: NaiveBayesModel
) extends Serializable {

  private def innerProduct (x : Array[Double], y : Array[Double]) : Double = {
    // remove NaN values
    x.zip(y).map(e => {
      val z = e._1 * e._2
      if (z.isNaN || z.isInfinity || z.isNegInfinity || z.isInfinite)
        0.00
      else
        z
    }).sum
  }

  val normalize = (u: Array[Double]) => {
    val uSum = u.sum
    u.map(e => e / uSum)
  }

  private val scoreArray = nb.pi.zip(nb.theta)
  /** Given a document string, return a vector of corresponding
    * class membership probabilities.
    * Helper function used to normalize probability scores.
    * Returns an object of type Array[Double]
    */
  private def getScores(doc: String): Array[Double] = {
    // Vectorize query
    val x: Vector = tfIdf.transform(doc)
    val z = scoreArray
      .map(e => innerProduct(e._2, x.toArray) + e._1)

    normalize((0 until z.size).map(k => exp(z(k) - z.max)).toArray)
  }

  /** Implement predict method for our model using
    * the prediction rule given in tutorial.
    */
  def predict(doc : String) : PredictedResults = {
    val x: Array[Double] = getScores(doc)
    val y = nb.labels zip x
    val sorted = y.sortWith(_._2 > _._2)
    val resultList = new ListBuffer[(PredictedResult)]()
    sorted.foreach(item => {
      if (item._2 > 0.001) resultList.append(PredictedResult(categoryMap.getOrElse(item._1, ""), item._2))
    })
    PredictedResults(resultList)
  }
}
