package commons.conf

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters


object ConfigurationManager {
  private val params = new Parameters()
  private val builder =
    new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
      .configure(params.properties()
        .setFileName("my.properties"))

  val config: FileBasedConfiguration = builder.getConfiguration()
}
