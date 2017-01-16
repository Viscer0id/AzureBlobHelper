import java.util
import com.microsoft.azure.storage.blob._
import org.joda.time.{DateTime, DateTimeZone}

object AzureBlob {
  def GetContainerSAS(container: CloudBlobContainer): String = {
    // Generate ad-hoc shared access signature for container access. ad-hoc because it is not being generated from a stored access policy.
    val sasConstraints = new SharedAccessBlobPolicy()
    // No start time means it will be valid immediately, expiry time set to 12 hours in the future
    val endDate = new DateTime(DateTimeZone.UTC).plusHours(12).toDate
    sasConstraints.setSharedAccessExpiryTime(endDate)
    sasConstraints.setPermissions(util.EnumSet.of(SharedAccessBlobPermissions.READ, SharedAccessBlobPermissions.WRITE))
    val sasContainerToken = container.generateSharedAccessSignature(sasConstraints, "default_12hr_RW")

    // Return the Container Uri with the Token as a single string
    container.getUri + sasContainerToken
  }
}
