package org.qcri.rheem.apps.util

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.core.plugin.{DynamicPlugin, Plugin}
import org.qcri.rheem.graphchi.GraphChi
import org.qcri.rheem.java.Java
import org.qcri.rheem.postgres.Postgres
import org.qcri.rheem.spark.Spark
import org.qcri.rheem.sqlite3.Sqlite3

/**
  * Utility to parse parameters of the apps.
  */
object Parameters {

  private val yamlId = """yaml\((.*)\)""".r

  val yamlPluginHel = "yaml(<YAML plugin URL>)"

  private val intPattern = """[+-]?\d+""".r

  private val longPattern = """[+-]?\d+L""".r

  private val doublePattern = """[+-]?\d+\.\d*""".r

  private val booleanPattern = """(?:true)|(?:false)""".r

  private val experiment =
    """exp\(([^,;]+)(?:;tags=([^,;]+(?:,[^,;]+)*))?(?:;conf=([^,;:]+:[^,;:]+(?:,[^,;:]+:[^,;:]+)*))?\)""".r

  val experimentHelp = "exp(<ID>[,tags=<tag>,...][,conf=<key>:<value>,...])"

  /**
    * Load a plugin.
    *
    * @param id name of the plugin
    * @return the loaded [[Plugin]]
    */
  def loadPlugin(id: String): Plugin = id match {
    case "basic-graph" => RheemBasics.graphPlugin
    case "java" => Java.basicPlugin
    case "java-graph" => Java.graphPlugin
    case "java-conversions" => Java.channelConversionPlugin
    case "spark" => Spark.basicPlugin
    case "spark-graph" => Spark.graphPlugin
    case "graphchi" => GraphChi.plugin
    case "postgres" => Postgres.plugin
    case "sqlite3" => Sqlite3.plugin
    case yamlId(url) => DynamicPlugin.loadYaml(url)
    case other => throw new IllegalArgumentException(s"Could not load platform '$other'.")
  }

  /**
    * Loads the specified [[Plugin]]s..
    *
    * @param platformIds a comma-separated list of platform IDs
    * @return the loaded [[Plugin]]s
    */
  def loadPlugins(platformIds: String): Seq[Plugin] = loadPlugins(platformIds.split(","))

  /**
    * Loads the specified [[Plugin]]s.
    *
    * @param platformIds platform IDs
    * @return the loaded [[Plugin]]s
    */
  def loadPlugins(platformIds: Seq[String]): Seq[Plugin] = platformIds.map(loadPlugin)

  /**
    * Create an [[Experiment]] for an experiment parameter and an [[ExperimentDescriptor]].
    *
    * @param experimentParameter  the parameter
    * @param experimentDescriptor the [[ExperimentDescriptor]]
    * @return the [[Experiment]]
    */
  def createExperiment(experimentParameter: String, experimentDescriptor: ExperimentDescriptor) =
  experimentParameter match {
    case experiment(id, tagList, confList) =>
      val tags = tagList match {
        case str: String => str.split(',').filterNot(_.isEmpty)
        case _ => Array[String]()
      }
      val experiment = experimentDescriptor.createExperiment(id, tags: _*)
      confList match {
        case str: String => str.split(',').map(_.split(':')).foreach { pair =>
          experiment.getSubject.addConfiguration(pair(0), parseAny(pair(1)))
        }
        case _ =>
      }
      experiment
    case other => throw new IllegalArgumentException(s"Could parse experiment descriptor '$other'.")
  }

  /**
    * Parses a given [[String]] into a specific basic type.
    *
    * @param str the [[String]]
    * @return the parsed value
    */
  private[util] def parseAny(str: String): AnyRef = {
    str match {
      case "null" => null
      case intPattern() => java.lang.Integer.valueOf(str)
      case longPattern() => java.lang.Long.valueOf(str.take(str.length - 1))
      case doublePattern() => java.lang.Double.valueOf(str)
      case booleanPattern() => java.lang.Boolean.valueOf(str)
      case other: String => other
    }
  }

}
