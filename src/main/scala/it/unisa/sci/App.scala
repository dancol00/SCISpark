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

    val sparkConf = new SparkConf().setAppName("Source Camera Identification")
    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(new Configuration())
    val cameraPath: String = "hdfs://masterunisa:9000/user/colucci/Dataset"
    val outputPath: String = "hdfs://masterunisa:9000/user/colucci/output"
    val cameraDirectory: Path = new Path(cameraPath)
    val cameraList: ArrayBuffer[(String, Image)] = new ArrayBuffer[(String, Image)]()
    val hdfsFolder = fs.listStatus(cameraDirectory)
    hdfsFolder.foreach(f => {
      if (f.isDirectory) {
        val tempCamera = new Camera(f.getPath)
        tempCamera.getImageList().foreach(image => cameraList += (tempCamera.getCameraName(), image))
      }
    })
    val cameraRDD = sc.parallelize(cameraList.toSeq)
    // TODO: add filenames (requires extending Image class?)
    val residualNoisesRDD = cameraRDD.map(cameraTuple => (cameraTuple._1, SCIManager.extractResidualNoise(cameraTuple._2)))
    val residualNoiseGroupedList = residualNoisesRDD.groupByKey.collect.to(ArrayBuffer)
    val rnForRP = ArrayBuffer[(String, ResidualNoise)]()
    for(camera <- residualNoiseGroupedList) {
      val noises = camera._2.to(ArrayBuffer)
      for(i <- 0 to sampleAmount) {
        val rand = Random.nextInt(camera._2.size)
        val rn = noises(rand)
        noises.remove(rand)
        rnForRP += (camera._1, rn)
      }
    }

    val tempRdd = sc.parallelize(rnForRP.toSeq)
    val referencePatternRddAddition = tempRdd.reduceByKey((a,b) => sumNoise(a, b): ResidualNoise)
    val referencePatternRdd: RDD[(String, ReferencePattern)] = referencePatternRddAddition.map(
      tuple => (tuple._1, divideNoise(tuple._2, sampleAmount.asInstanceOf[Float]))
    )

    val compareRdd = residualNoisesRDD.cartesian(referencePatternRdd)
    // Structure of rdd: Camera name, Residual noise, RP camera name, Reference Pattern
    // Structure after comparison: Camera name, RP name, correlation
    val comparison = compareRdd.map(tuples => (tuples._1._1, tuples._2._1, SCIManager.compare(tuples._2._2, tuples._1._2)))

    comparison.saveAsTextFile(outputPath)


    /*
    val referencePatternsRDD = cameraRDD.map(camera => (camera, camera.extractReferencePattern()))

    val residualNoiseFilesBadList: Array[(String, ArrayBuffer[File])] = referencePatternsRDD.map(
      camera => (camera._1.getCameraName(), camera._1.getResidualNoiseFiles())).collect()

    // flatmap?
    // val residualNoiseFilesRDD = residualNoisePatternsRDD.map(tuple => (tuple._1, tuple._1.getResidualNoiseFiles()))

    var residualNoiseFilesListUnpacked: ArrayBuffer[(String, File)] = new ArrayBuffer[(String, File)]()

    for(tuple <- residualNoiseFilesBadList) {
      val cameraName = tuple._1
      val listInTuple = tuple._2
      for(file <- listInTuple) {
        residualNoiseFilesListUnpacked +:= (cameraName, file)
      }
    }
    //residualNoiseFilesBadList.foreach(tuple => tuple._2.foreach(item => residualNoiseFilesListUnpacked += (String, item)))

    val residualNoiseFilesRDD = sc.parallelize(residualNoiseFilesListUnpacked.toSeq)

    val residualNoiseRDD = residualNoiseFilesRDD.map(fileTuple => (fileTuple._1, fileTuple._2.getName, SCIManager.extractResidualNoise(new Image(fileTuple._2))))

    val compareRDD = residualNoiseRDD.cartesian(referencePatternsRDD);

    // Struttura: Nome fotocamera Residual Noise, Nome file Residual Noise, Nome fotocamera Reference Pattern, Indice di correlazione
    val finalRDD = compareRDD.map(tuple => (tuple._1._1, tuple._1._2, tuple._2._1.getCameraName(), SCIManager.compare(tuple._2._2, tuple._1._3) ))

    finalRDD.saveAsTextFile("...") //TODO: Aggiungere file di output HDFS
     */
  }

  private def sumNoise(rn1: ResidualNoise, rn2: ResidualNoise): ResidualNoise = {
    rn1.add(rn2)
    rn1
  }
  private def divideNoise(residualNoise: ResidualNoise, value: Float): ReferencePattern = {
    residualNoise.divideByValue(value)
    val rp: ReferencePattern = new ReferencePattern(residualNoise)
    rp
  }

}
