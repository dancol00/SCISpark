package it.unisa.sci

import it.unisa.di.dif.SCIManager
import it.unisa.di.dif.pattern.Image
import org.apache.spark.rdd.RDD

import java.io.File
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}

final class App {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Source Camera Identification")
    val sc = new SparkContext(sparkConf)


    val cameraPath: String = "..." //TODO: aggiungere file input HDFS

    val cameraDirectory: File = new File(cameraPath);
    assert(cameraDirectory.isDirectory)

    val cameraList: ArrayBuffer[Camera] = new ArrayBuffer[Camera]()

    cameraDirectory.listFiles().foreach(f => {
      if (f.isDirectory) {
        cameraList += new Camera(f)
      }
    })

    val cameraRDD = sc.parallelize(cameraList.toSeq)

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
  }
}
