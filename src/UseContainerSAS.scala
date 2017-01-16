import java.util

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.JavaConversions._

object UseContainerSAS {
  def main(args: Array[String]): Unit = {
    val dataSourceName = "Default"

    // Create spark session
    val spark = SparkSession.builder.appName("Processing: " + dataSourceName).getOrCreate()

    // Load the application configuration supplied by the application.conf resource bundled in the jar
    val unprocAcctCxnString: String = ConfigFactory.load().getString("configuration.storage.unprocessed.connectionString")
    val unprocContainerName: String = ConfigFactory.load().getString("configuration.storage.unprocessed.container")

    // Create Azure account connections
    val unProcAcct = CloudStorageAccount.parse(unprocAcctCxnString)

    // Create Azure container handles
    val unProcContainer = unProcAcct.createCloudBlobClient().getContainerReference(unprocContainerName)

    // Create Azure container handles with SAS
    val unProcContainerSAS = unProcAcct.createCloudBlobClient().getContainerReference(AzureBlob.GetContainerSAS(unProcContainer))

    // Create an array of blobs for processing
    println("Creating array for processing")
    val blobList = unProcContainerSAS.listBlobs(dataSourceName, true)
    val sourceBlobs = blobList.map(x => x.asInstanceOf[CloudBlockBlob])

    // List files we are going to process
    val sourceBlobNames = sourceBlobs
      .map(row => row.getName)
      .toArray[String]
    println("List of files:")
    println(sourceBlobNames.mkString(System.lineSeparator()))
  }
}

//    for (sourceBlob <- sourceBlobs) {
//      print(sourceBlob.getName)
//      val targetBlob = archivedContainerSAS.getBlockBlobReference(sourceBlob.getName)
//      try {
//        // Copy is asynchronous
//        val copyToken: String = targetBlob.startCopy(sourceBlob)
//        println("Copy task ID: " + copyToken)
//        println("Status: " + targetBlob.getCopyState.getStatus)
//        if (targetBlob.getCopyState.getStatus == CopyStatus.SUCCESS) {
//          println("Copy successful, deleting source blob")
//        }
//      } catch {
//        case e: com.microsoft.azure.storage.StorageException => println(e.getExtendedErrorInformation.getErrorMessage)
//      }
//    }
