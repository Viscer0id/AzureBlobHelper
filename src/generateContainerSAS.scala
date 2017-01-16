import java.util
import com.microsoft.azure.storage.{CloudStorageAccount}
import com.microsoft.azure.storage.blob._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.joda.time.{DateTime, DateTimeZone}
import scala.collection.JavaConversions._

object generateContainerSAS {
  def main(args: Array[String]): Unit = {
    //    val Array(dataSourceName: String, processMode: String, fileWriteMode: String) = args

    val dataSourceName = "Default"

    // Create spark session
    val spark = SparkSession.builder.appName("Processing: "+dataSourceName).getOrCreate()

    // Load the application configuration supplied by the application.conf resource bundled in the jar
    val unprocAcctCxnString: String = ConfigFactory.load().getString("configuration.storage.unprocessed.connectionString")
    val unprocContainerName: String = ConfigFactory.load().getString("configuration.storage.unprocessed.container")

    val archivedAcctCxnString: String = ConfigFactory.load().getString("configuration.storage.archived.connectionString")
    val archivedContainerName: String = ConfigFactory.load().getString("configuration.storage.archived.container")

    // Create Azure account connections
    val unProcAcct =  CloudStorageAccount.parse(unprocAcctCxnString)
    val archiveAcct = CloudStorageAccount.parse(archivedAcctCxnString)

    // Create Azure container handles
    val unProcContainer = unProcAcct.createCloudBlobClient().getContainerReference(unprocContainerName)
    val archivedContainer = archiveAcct.createCloudBlobClient().getContainerReference(archivedContainerName)

    // Create Azure container handles with SAS
    val unProcContainerSAS = unProcAcct.createCloudBlobClient().getContainerReference(getContainerSASUri(unProcContainer))
    val archivedContainerSAS = archiveAcct.createCloudBlobClient().getContainerReference(getContainerSASUri(archivedContainer))

    // Create an array of blobs for processing
    //    val blobClient = unProcAcct.createCloudBlobClient().getContainerReference(unprocContainer)
    println("Creating array for processing")
    val blobList = unProcContainerSAS.listBlobs(dataSourceName,true)
    val sourceBlobs = blobList.map(x => x.asInstanceOf[CloudBlockBlob])

    // Debugging info, list the files we are going to process
    val sourceBlobNames = sourceBlobs
      .map(row => row.getName)
      .toArray[String]
    println("List of files to process:")
    println(sourceBlobNames.mkString(System.lineSeparator()))

    for (sourceBlob <- sourceBlobs) {
      print(sourceBlob.getName)
      val targetBlob = archivedContainerSAS.getBlockBlobReference(sourceBlob.getName)
      try {
        // Copy is asynchronous
        val copyToken: String = targetBlob.startCopy(sourceBlob)
        println("Copy task ID: " + copyToken)
        println("Status: " + targetBlob.getCopyState.getStatus)
        if (targetBlob.getCopyState.getStatus == CopyStatus.SUCCESS) {
          println("Copy successful, deleting source blob")
        }
      } catch {
        case e: com.microsoft.azure.storage.StorageException => println(e.getExtendedErrorInformation.getErrorMessage)
      }
    }
  }

  def getContainerSASUri(container: CloudBlobContainer): String = {
    // Generate ad-hoc shared access signature for container access. ad-hoc because it is not being generated from a stored access policy.
    val sasConstraints = new SharedAccessBlobPolicy()
    val endDate = new DateTime(DateTimeZone.UTC).plusHours(2).toDate
    sasConstraints.setSharedAccessExpiryTime(endDate)
    sasConstraints.setPermissions(util.EnumSet.of(SharedAccessBlobPermissions.READ,SharedAccessBlobPermissions.WRITE))
    val sasContainerToken = container.generateSharedAccessSignature(sasConstraints,"default")

    container.getUri + sasContainerToken
  }
}
