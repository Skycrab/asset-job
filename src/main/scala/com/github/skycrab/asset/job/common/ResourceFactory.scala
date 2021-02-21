package com.github.skycrab.asset.job.common

/**
  * Created by yihaibo on 2019-12-23.
  */
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.env.{PropertiesPropertySource, PropertySource}
import org.springframework.core.io.support.{DefaultPropertySourceFactory, EncodedResource}

class ResourceFactory extends DefaultPropertySourceFactory {

  override def createPropertySource(name: String, resource: EncodedResource): PropertySource[_] = {
    val sourceName = if (name == null) resource.getResource.getFilename else name
    assert(sourceName != null)
    if (sourceName.endsWith(".yml") || sourceName.endsWith(".yaml")) {
      val factory = new YamlPropertiesFactoryBean
      factory.setResources(resource.getResource)
      factory.afterPropertiesSet()
      val properties = factory.getObject
      assert(properties != null)
      return new PropertiesPropertySource(sourceName, properties)
    }
    super.createPropertySource(name, resource)
  }
}