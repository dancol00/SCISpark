package it.unisa.sci

import it.unisa.di.dif.SCIManager
import it.unisa.di.dif.pattern.{Image, ReferencePattern, ResidualNoise}
import org.apache.hadoop.conf.Configuration

import java.io.{BufferedInputStream, File, InputStream}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

class Camera {

  private var cameraName: String = ""
  private var referencePattern: ReferencePattern = null
  private var residualNoises = ArrayBuffer[ResidualNoise]()

  private val imagePaths: ArrayBuffer[Path] = ArrayBuffer[Path]()
  private val imageList: ArrayBuffer[CustomImage] = ArrayBuffer[CustomImage]()
  private val residualNoiseFiles: ArrayBuffer[Image] = ArrayBuffer[Image]()

  def this(folder: Path) = {
    this()
    cameraName = folder.getName
    val fs = FileSystem.get(new Configuration())
    val imgFolder: Path = new Path(folder.toString + "/img/")
    val images = fs.listStatus(imgFolder)
    images.foreach(f => {
      val path = f.getPath
      imageList += new CustomImage(fs.open(path), cameraName, path.getName)
    })
  }

  def getCameraName(): String = {
    cameraName
  }

  def getImagePaths(): ArrayBuffer[Path] = {
    imagePaths
  }

  def getImageList(): ArrayBuffer[CustomImage] = {
    imageList
  }

  def getResidualNoiseFiles(): ArrayBuffer[Image] = {
    residualNoiseFiles
  }

  def getResidualNoises(): ArrayBuffer[ResidualNoise] = {
    residualNoises
  }

  def extractResidualNoise(image: Image): ResidualNoise = {
    var noise: ResidualNoise = null
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

  /*
  def extractReferencePattern(samples: Int = 60): ReferencePattern = {
    var referencePatternUtility: ArrayBuffer[Path] = new ArrayBuffer[Path]()

    for(i <- 0 to samples) {
      val randomNumber: Int = scala.util.Random.nextInt(residualNoiseFiles.size)
      referencePatternUtility += residualNoiseFiles(randomNumber)
      residualNoiseFiles.remove(randomNumber)
    }
    referencePattern = SCIManager.extractReferencePattern(referencePatternUtility.asJava)

    referencePattern
  }
   */
}
