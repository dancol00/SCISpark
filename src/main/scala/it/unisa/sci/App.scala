package it.unisa.sci

import it.unisa.di.dif.SCIManager
import it.unisa.di.dif.pattern.{Image, ReferencePattern, ResidualNoise}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.Path

import scala.util.Random


object App {

  def main(args: Array[String]): Unit = {

    val sampleAmount: Integer = 60
    val cameraPath: String = "hdfs://masterunisa:9000/user/colucci/Dataset/DatasetSmartphone/Foto"
    val outputPath: String = "hdfs://masterunisa:9000/user/colucci/output"

    val sparkConf = new SparkConf().setAppName("Source Camera Identification")
    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(new Configuration())
    val cameraDirectory: Path = new Path(cameraPath)

    //val imageList = new ArrayBuffer[(String, String, Path)]()
    val rnImageList: ArrayBuffer[(String, String, Path)] = new ArrayBuffer[(String, String, Path)]()
    val rpImageList: ArrayBuffer[(String, Path)] = new ArrayBuffer[(String, Path)]()

    val hdfsFolder = fs.listStatus(cameraDirectory)
    hdfsFolder.foreach(f => {
      if (f.isDirectory) {
        val cameraName = f.getPath.getName
        val images = fs.listStatus(new Path(f.getPath, "img"))
        images.foreach(image => {
          val path = image.getPath
          rnImageList += ((cameraName, path.getName, path))
        })

        for (i <- 0 to sampleAmount) {
          val rand = Random.nextInt(rnImageList.size)
          val rpImage = rnImageList(rand)
          rnImageList.remove(rand)
          rpImageList += ((rpImage._1, rpImage._3)) // Filename is not needed for Reference Pattern
        }
      }
    })
    // TODO: Creare liste dei file, un RDD per fotocamera, estrarre immagini per RP rimuovendo le foto da RDD
    // TODO: Successivamente creare un unico RDD per RN e suddividere l'estrazione e correlazione ogni x immagini (es. take 50)
    // TODO: Vedere se fare map(estrai RN).reduceByKey(add) risolve il problema della memoria
    // TODO: aggregateByKey invece di seconda map a rpRddComputed?
    // TODO: add filenames (requires extending Image class?)

    val rpRdd = sc.parallelize(rpImageList)
    /*
    val rpRddComputed = rpRdd.map(tuple=> (tuple._1, SCIManager.extractResidualNoise(tuple._2)))
      .reduceByKey((rp1, rp2) => sumNoise(rp1, rp2))
      .map(tuple => (tuple._1, divideNoise(tuple._2, sampleAmount.floatValue())))

     */
    val startRP = new ReferencePattern()
    val rpRddComputed = rpRdd.aggregateByKey(startRP)(extractSum, sumNoise)
      .map(tuple => (tuple._1, divideNoise(tuple._2, sampleAmount.floatValue())))

    println("\n\n///// EXTRACTED REFERENCE PATTERNS /////\n\n")

    val referencePatterns = sc.broadcast(rpRddComputed.collect())

    println("\n\n///// BROADCASTED REFERENCE PATTERNS /////\n\n")

    val rnRdd = sc.parallelize(rnImageList)
    val correlation = rnRdd.flatMap(rnTuple => {
      val correlationList = new ArrayBuffer[(String, String, String, Double)]()
      referencePatterns.value.foreach(tuple => {
        val image = new Image(fs.open(rnTuple._3))
        correlationList += ((
          rnTuple._1, // Camera name
          rnTuple._2, // File name
          tuple._1,   // Reference Pattern Camera name
          SCIManager.compare(tuple._2, SCIManager.extractResidualNoise(image)) // Correlation
        ))
      })
      correlationList
    })

    correlation.saveAsTextFile(outputPath)
  }

  private def extractSum(rp1: ReferencePattern, path: Path): ReferencePattern = {
    val fs = FileSystem.get(new Configuration())
    val image = new Image(fs.open(path))
    val rp2 = new ReferencePattern(SCIManager.extractResidualNoise(image))
    sumNoise(rp1, rp2)
  }
  private def extractSumAndDivide(rp1: ReferencePattern, image: Image): ReferencePattern = {
    val rp2 = new ReferencePattern(SCIManager.extractResidualNoise(image))
    divideNoise(sumNoise(rp1, rp2), 2)
  }

  private def extractSumAndDivide(image1: Image, image2: Image): ReferencePattern = {
    divideNoise(sumNoise(
      SCIManager.extractResidualNoise(image1),
      SCIManager.extractResidualNoise(image2)
    ), 2)
  }
  private def sumAndDivide(rn1: ReferencePattern, rn2: ReferencePattern): ReferencePattern = {
    divideNoise(sumNoise(rn1, rn2), 2)
  }

  private def sumNoise(rn1: ReferencePattern, rn2: ReferencePattern): ReferencePattern = {
    rn1.add(rn2)
    rn1
  }
  private def sumNoise(rn1: ResidualNoise, rn2: ResidualNoise): ReferencePattern = {
    rn1.add(rn2)
    val rp: ReferencePattern = new ReferencePattern(rn1)
    rp
  }
  private def divideNoise(residualNoise: ResidualNoise, value: Float): ReferencePattern = {
    residualNoise.divideByValue(value)
    val rp: ReferencePattern = new ReferencePattern(residualNoise)
    rp
  }

  private def divideNoise(rp: ReferencePattern, value: Float): ReferencePattern = {
    rp.divideByValue(value)
    rp
  }

  private def compareToList(residualNoise: ResidualNoise, referencePatterns: Array[(String, ReferencePattern)]): List[(String, Double)] = {
    val compareList = new ArrayBuffer[(String, Double)]
    referencePatterns.foreach(rp => compareList += ((rp._1, SCIManager.compare(rp._2, residualNoise))))
    compareList.toList
  }

}
