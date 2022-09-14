/*
 * CS-543 Project Spring 2021
 *
 * George-Stavros Pantelakis (csd4017)
 * Evangelia Skouloudi (csd4039)
 * Michail Raptakis (csd4101)
 *
 * parser.scala
 * Main parser for NYC-Taxi dataset from Open Data of AWS
 * Includes exporting of statistical data
 */
import org.apache.spark
import java.io._
import scala.reflect.io.Directory
import org.apache.spark.rdd.RDD
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import scala.collection.immutable.ListMap

case class Entry (pickup_datetime: LocalDateTime, dropoff_datetime: LocalDateTime, passenger_count: Int, trip_distance: Float, puLocationID: Int, doLocationID: Int, payment_type: Int, fare_amount: Float, extra: Float, mta_tax: Float, tip_amount: Float, tolls_amount: Float, improvement_surcharge: Float, total_amount: Float)

def getListOfFiles():List[String] = {
	var listOfFiles = List[String]();
	for(year <- 2014 until 2021)
		for(month <- 1 until 13)
			if(month < 10)
				listOfFiles = s"s3://nyc-tlc/trip data/yellow_tripdata_${year}-0${month}.csv" :: listOfFiles;
			else
				listOfFiles = s"s3://nyc-tlc/trip data/yellow_tripdata_${year}-${month}.csv" :: listOfFiles;
	listOfFiles
}

def parser(str: String, header: Array[String]) : Entry = {
    var pickup_datetime: LocalDateTime = null
    var dropoff_datetime: LocalDateTime = null
    var passenger_count = 0
    var trip_distance = 0f
    var puLocationID = 0
    var doLocationID = 0
    var payment_type = 5
    var fare_amount = 0f
    var extra = 0f
    var mta_tax = 0f 
    var tip_amount = 0f
    var tolls_amount = 0f
    var improvement_surcharge = 0f
    var total_amount = 0f

    val df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val data = str.split(",")
    for (i <- 0 until header.length){
        val key = header(i).trim

        if(key contains "pickup_datetime"){
            pickup_datetime = LocalDateTime.parse(data(i), df)
        }
        else if(key contains "dropoff_datetime"){
            dropoff_datetime = LocalDateTime.parse(data(i), df)
        }
        else if(key == "passenger_count"){
            if(data(i) != ""){
                passenger_count = data(i).toInt
            }
        }
        else if(key == "trip_distance"){
            if(data(i) != ""){
                trip_distance = data(i).toFloat
            }
        }
        else if(key == "PULocationID"){
            if(data(i) != ""){
                puLocationID = data(i).toInt
            }
        }
        else if(key == "DOLocationID"){
            if(data(i) != ""){
                doLocationID = data(i).toInt
            }
        }
        else if(key == "payment_type"){
            if(data(i) != ""){
                if(data(i).forall(_.isDigit)){
                    payment_type = data(i).toInt
                }
                else if(data(i) == "CRD"){
                    payment_type = 1
                }
            }
        }
        else if(key == "fare_amount"){
            if(data(i) != ""){
                fare_amount = data(i).toFloat
            }
        }
        else if((key == "extra") || (key == "surcharge")){
            if(i < data.size){
                if(data(i) != ""){
                    extra = data(i).toFloat
                }
            }
        }
        else if(key == "mta_tax"){
            if(data(i) != ""){
                mta_tax = data(i).toFloat
            }
        }
        else if(key == "tip_amount"){
            if(data(i) != ""){
                tip_amount = data(i).toFloat
            }
        }
        else if(key == "tolls_amount"){
            if(data(i) != ""){
                tolls_amount = data(i).toFloat
            }
        }
        else if(key == "improvement_surcharge"){
            if(data(i) != ""){
                improvement_surcharge = data(i).toFloat
            }
        }
        else if(key == "total_amount"){
            if(data(i) != ""){
                total_amount = data(i).toFloat
            }
        }
    }

    return Entry(pickup_datetime, dropoff_datetime, passenger_count, trip_distance, puLocationID, doLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount)
}

def parseAllFiles(listOfFiles: List[String]) : RDD[Entry] = {
    var parsedRdd = sc.emptyRDD[Entry]

    for (file <- listOfFiles) {
        var baseRdd = sc.textFile(file)
        val header = baseRdd.take(1)(0).split(",")
        baseRdd = baseRdd.mapPartitionsWithIndex(
                   (index, it) => if (index == 0) it.drop(1) else it
                 )
        val tempRdd = baseRdd.filter(_ != "").map(line => parser(line, header))
        parsedRdd = parsedRdd.union(tempRdd)
    }

    return parsedRdd
}

val parsedRdd = parseAllFiles(getListOfFiles()).cache
parsedRdd.count

// Distance distribution
val distanceDistribution = parsedRdd.map(entry => ((entry.pickup_datetime.getYear.toString, entry.pickup_datetime.getMonth.toString), entry.trip_distance)).groupByKey.map(entry => (entry._1, entry._2.reduceLeft(_ + _)))

// Passenger distribution
val passengerDistribution = parsedRdd.map( x => (x.pickup_datetime, x.passenger_count)).map(x => (x._1.getYear, x._1.getMonth, x._2)).groupBy(x => (x._1, x._2)).map(x => (x._1, x._2.reduceLeft((l, z) => (l._1, l._2, l._3 + z._3)))).map(x => x._2)

// Busy Hours per Day of Week
val busyHours = parsedRdd.map(x => (x.pickup_datetime.getDayOfWeek, x.pickup_datetime.getHour)).groupByKey.map(x => (x._1, x._2.groupBy(identity))).map(x => (x._1 ,x._2.map(y => (y._1 , y._2.size)))).map(x=> (x._1 , ListMap(x._2.toSeq.sortWith(_._1 < _._1):_*)))

// Paying way through the years
val payingType = parsedRdd.map(x => (x.pickup_datetime.getYear, x.pickup_datetime.getMonth, x.payment_type)).groupBy(x => (x._1, x._2)).map(x => (x._1, x._2.groupBy(identity))).map(x => (x._1 ,x._2.map(y => (y._1._3 , y._2.size)))).map(x=> (x._1 , ListMap(x._2.toSeq.sortWith(_._1 < _._1):_*)))

// Profitable areas
val profitableAreas = parsedRdd.filter(_.puLocationID != 0).map(x => (x.puLocationID, x.fare_amount.toDouble - (x.trip_distance.toDouble * 0.15) + x.tip_amount.toDouble)).groupByKey.map(x => (x._1, x._2.reduceLeft(_ + _)))

//creating plot files for busy hours
busyHours.map(elem => {
    (-1 to 23).map(x => if(x == -1) "" else x.toString).reduceLeft((res, hour) => {
        if (elem._2.contains(hour.toInt))
            res + elem._1.toString + "\t" + hour.toString + " " + elem._2(hour.toInt).toString + "\n"
        else
            res + elem._1.toString + "\t" + hour.toString + " 0\n"
    })
}).saveAsTextFile("./plotData/busyHours.data")

//creating plot files for distance distribution
distanceDistribution.map(elem => {
    elem._1._1.toString + "\t" + elem._1._2.toString + "\t" + elem._2.toString
}).saveAsTextFile(s"./plotData/distanceDistribution.data")

//creating plot files for payment type
payingType.map(elem => {
    elem._1._1.toString + "\t" + elem._1._2.toString + "\t" + (if(elem._2.contains(1)) elem._2(1).toString else "0") + "\t" + (if(elem._2.contains(2)) elem._2(2).toString else "0")
}).saveAsTextFile(s"./plotData/payingType.data")

//creating plot files for passenger distribution
passengerDistribution.map(elem => {
    elem._1.toString + "\t" + elem._2.toString + "\t" + elem._3.toString
}).saveAsTextFile( s"./plotData/passengerDistribution.data")

//creating plot files for profitable areas
profitableAreas.map(x => (x._1 + "\t" + x._2)).saveAsTextFile("./plotData/profitableAreas.data")