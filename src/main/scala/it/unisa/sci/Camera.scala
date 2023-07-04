package it.unisa.sci

import it.unisa.di.dif.SCIManager
import it.unisa.di.dif.pattern.{Image, ReferencePattern, ResidualNoise}

import java.io.File
import java.nio.file.Path
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.ArrayBuffer

class Camera {

  private var cameraName: String = ""
  private var referencePattern: ReferencePattern = null
  private var residualNoises = ArrayBuffer[ResidualNoise]()

  private var residualNoiseFiles: ArrayBuffer[File] = ArrayBuffer[File]()

  def this(folder: File) = {
    this()
    cameraName = folder.getName
    val imgFolder = new File(folder.getPath, "/img/")
    assert(imgFolder.exists())
    imgFolder.listFiles().foreach(f => residualNoiseFiles +:= f)
  }

  def getCameraName(): String = {
    cameraName
  }

  def getResidualNoiseFiles(): ArrayBuffer[File] = {
    residualNoiseFiles
  }

  def getResidualNoises(): ArrayBuffer[ResidualNoise] = {
    residualNoises
  }

  def extractResidualNoise(f: File): ResidualNoise = {
    var noise: ResidualNoise = null
    val image = new Image(f)
    noise = SCIManager.extractResidualNoise(image)
    noise
  }

  def extractAllResidualNoises(): ArrayBuffer[ResidualNoise] = {
    residualNoiseFiles.foreach(file => {
      residualNoises +:= extractResidualNoise(file)
      }
    )
    residualNoises
  }

  def extractReferencePattern(samples: Int = 60): ReferencePattern = {
    var referencePatternUtility: ArrayBuffer[Path] = new ArrayBuffer[Path]()

    for(i <- 0 to samples) {
      val randomNumber: Int = scala.util.Random.nextInt(residualNoiseFiles.size)
      referencePatternUtility +:= residualNoiseFiles(randomNumber).toPath
      residualNoiseFiles.remove(randomNumber)
    }
    referencePattern = SCIManager.extractReferencePattern(referencePatternUtility.asJava)

    referencePattern
  }
}
