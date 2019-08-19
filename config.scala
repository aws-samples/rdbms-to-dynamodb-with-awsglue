import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hive.dynamodb.`type`.HiveDynamoDBItemType
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ParIterableLike

import com.google.common.collect.Maps
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.document.{Item, ItemUtils}
import com.amazonaws.services.glue.{GlueContext, MappingSpec, DynamicFrame}
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.errors.CallSite

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.{SQLContext, SparkSession, DataFrame}

object GlueWriteToDynamoDB {
    def main(sysArgs: Array[String]): Unit = {
        val sc: SparkContext = new SparkContext()
        val glueContext: GlueContext = new GlueContext(sc)
        sc.setLogLevel("ERROR")
        
        var jssc = new JavaSparkContext(sc);
        jssc.hadoopConfiguration().set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
        
        // Glue Database
        val dbName = "gluedatabase"
        
        val tablePrefix = "crawler_ss_dms_sample_dbo_"

        // Creating Dynamic Frames
        val ddfPerson: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tablePrefix + "person").getDynamicFrame()
        val ddfSportTeam: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tablePrefix + "sport_team").getDynamicFrame()
        val ddfSportType: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tablePrefix + "sport_type").getDynamicFrame()
        val ddfSportLocation: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tablePrefix + "sport_location").getDynamicFrame()
        val ddfSportDivision: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tablePrefix + "sport_division").getDynamicFrame()
        val ddfSportLeague: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tablePrefix + "sport_league").getDynamicFrame()

        // Creating Data Frames
        val dfPerson = ddfPerson.toDF()
        val dfSportTeam = ddfSportTeam.toDF()
        val dfSportType = ddfSportType.toDF()
        val dfSportLocation = ddfSportLocation.toDF()
        val dfSportDivision = ddfSportDivision.toDF()
        val dfSportLeague = ddfSportLeague.toDF()
        
        dfPerson.createOrReplaceTempView("person")
        dfSportTeam.createOrReplaceTempView("sport_team")
        dfSportType.createOrReplaceTempView("sport_type")
        dfSportLocation.createOrReplaceTempView("sport_location")
        dfSportDivision.createOrReplaceTempView("sport_division")
        dfSportLeague.createOrReplaceTempView("sport_league")

        val spark = SparkSession
            .builder()
            .appName("ConfigExample")
            .getOrCreate()

        // Creating a Spark SQL and writing to DynamoDB
        val person = spark.sql("""
        SELECT 'PERSON' AS data_type, 
        CAST(id AS STRING) AS data_id, 
        full_name AS descriptive_name, 
        last_name, 
        first_name 
        FROM person LIMIT 1000
        """)
        writeToDynamoDB("config", person, sc);

        val sportTeam = spark.sql("""
        SELECT 'TEAM' AS data_type, 
        CAST(id AS STRING) AS data_id, 
        name AS descriptive_name, 
        abbreviated_name AS team_name_abbrev, 
        home_field_id AS team_home_field_id, 
        sport_type_name AS team_sport_type_name, 
        concat(sport_league_short_name,'|',sport_division_short_name) AS team_sport_league 
        FROM sport_team
        """)
        writeToDynamoDB("config", sportTeam, sc);

        val sportType = spark.sql("""
        SELECT 'SPORT_TYPE' AS data_type, 
        name AS data_id, 
        description AS descriptive_name 
        FROM sport_type
        """)
        writeToDynamoDB("config", sportType, sc);

        val location = spark.sql("""
        SELECT 'LOCATION' AS data_type, 
        CAST(id AS STRING) AS data_id, 
        name AS descriptive_name, 
        city, 
        seating_capacity, 
        levels, 
        sections  
        FROM sport_location
        """)
        writeToDynamoDB("config", location, sc);

        val sportLeagueDivision = spark.sql("""
        SELECT 
        'SPORT_LEAGUE_DIVISION' AS data_type
        ,concat(sport_league.short_name,'|',sport_division.short_name) AS data_id
        ,concat(sport_league.long_name,' ',sport_division.long_name) AS descriptive_name
        FROM sport_division
        JOIN sport_league ON sport_division.sport_type_name=sport_league.sport_type_name AND sport_division.sport_league_short_name=sport_league.short_name
        """)
        writeToDynamoDB("config", sportLeagueDivision, sc);
    }

    def writeToDynamoDB(ddbTableName: String, dataFrame: DataFrame, sc: SparkContext){
        var jobConf = new JobConf(sc.hadoopConfiguration)
        jobConf.set("dynamodb.output.tableName", ddbTableName)
        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

        var itemList = new ListBuffer[(org.apache.hadoop.io.Text, DynamoDBItemWritable)]()
        
        val joinedDFJson = dataFrame.toJSON
        
        joinedDFJson.collect().foreach{ item =>
            val json = Item.fromJSON(item)
            val itemMap = json.asMap()
            val itemActualMap = ItemUtils.fromSimpleMap(itemMap)
            
            val dynamoDBItemWritable = new DynamoDBItemWritable()
            dynamoDBItemWritable.setItem(itemActualMap)
            val itemTuple = new Tuple2(null, dynamoDBItemWritable)
            itemList += itemTuple
        }

        val myrdd = sc.parallelize(itemList.toList)
        myrdd.saveAsHadoopDataset(jobConf)
    }
}