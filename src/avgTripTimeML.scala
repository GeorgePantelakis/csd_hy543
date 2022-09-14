/*
 * CS-543 Project Spring 2021
 *
 * George-Stavros Pantelakis (csd4017)
 * Evangelia Skouloudi (csd4039)
 * Michail Raptakis (csd4101)
 *
 * avgTripTimeML.scala
 * Training and evaluating process for average trip time linear regression model
 */
import java.time.Duration
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{LabeledPoint => MLabeledPoint}
import org.apache.spark.ml.linalg.{Vectors => MLVectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

def labeledPointTransform(entry: Entry): LabeledPoint = {
	// find total time
	val total_time = Duration.between(entry.pickup_datetime, entry.dropoff_datetime).toMinutes.abs;
	LabeledPoint(total_time.toDouble,
		Vectors.dense(entry.pickup_datetime.toEpochSecond(ZoneOffset.UTC).toDouble,
			      entry.trip_distance.toDouble,
			      entry.puLocationID.toDouble,
			      entry.doLocationID.toDouble))
}

val lpRdd = parsedRdd.map(labeledPointTransform)

val weights = Array(.8, .1, .1)
val seed = 123
val Array(trainData, valData, testData) = lpRdd.randomSplit(weights, seed)
val trainDataDF = trainData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
val valDataDF = valData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
val testDataDF = testData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF

// training
val evaluator = new RegressionEvaluator()
evaluator.setMetricName("rmse")

val lr = new LinearRegression().setFitIntercept(true)
val grid = new ParamGridBuilder().addGrid(lr.maxIter, Array(500, 1000, 1500)).addGrid(lr.regParam, Array(1e-5, 1e-10, 1e-15)).addGrid(lr.elasticNetParam, Array(.2, .5, .7)).build()
val lr_cv = new CrossValidator().setEstimator(lr).setEstimatorParamMaps(grid).setEvaluator(evaluator).setNumFolds(3).setParallelism(2)
val lrModels = lr_cv.fit(trainDataDF)
val bestModel = lrModels.bestModel

val valPredictionsDF = bestModel.transform(valDataDF)
val testPredictionsDF = bestModel.transform(testDataDF)

testPredictionsDF.select("prediction").show
println(s"RMSE on val set: ${evaluator.evaluate(valPredictionsDF)}")
println(s"RMSE on test set: ${evaluator.evaluate(testPredictionsDF)}")
println(s"Coeffs: ${bestModel.asInstanceOf[LinearRegressionModel].coefficients}\\Intercept: ${bestModel.asInstanceOf[LinearRegressionModel].intercept}")

bestModel.asInstanceOf[LinearRegressionModel].save("./avgTripTimeModel")