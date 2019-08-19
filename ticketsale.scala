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
        
        val table_prefix="crawler_ss_dms_sample_dbo_"

        // Creating Dynamic Frames
        val ddfTicketPurchaseHist: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = table_prefix+"ticket_purchase_hist").getDynamicFrame()
        val ddfPerson: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = table_prefix+"person").getDynamicFrame()
        val ddfSportingEventTicket: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = table_prefix+"sporting_event_ticket").getDynamicFrame()
        val ddfSportingEvent: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = table_prefix+"sporting_event").getDynamicFrame()
        val ddfSportLocation: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = table_prefix+"sport_location").getDynamicFrame()
        val ddfSportTeam: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = table_prefix+"sport_team").getDynamicFrame()

        // Creating Data Frames
        val dfTicketPurchaseHist = ddfTicketPurchaseHist.toDF()
        val dfPerson = ddfPerson.toDF()
        val dfSportingEventTicket = ddfSportingEventTicket.toDF()
        val dfSportingEvent = ddfSportingEvent.toDF()
        val dfSportLocation = ddfSportLocation.toDF()
        val dfSportTeam = ddfSportTeam.toDF()
        
        dfTicketPurchaseHist.createOrReplaceTempView("ticket_purchase_hist")
        dfPerson.createOrReplaceTempView("person")
        dfSportingEventTicket.createOrReplaceTempView("sporting_event_ticket")
        dfSportingEvent.createOrReplaceTempView("sporting_event")
        dfSportLocation.createOrReplaceTempView("sport_location")
        dfSportTeam.createOrReplaceTempView("sport_team")

        val spark = SparkSession
            .builder()
            .appName("TicketSaleExample")
            .getOrCreate()

        val ticketDF = spark.sql("""
        SELECT sporting_event_ticket_id
        ,CASE
            WHEN purchased_by_id IS NULL THEN 'Created'
            WHEN transferred_from_id IS NULL THEN CONCAT('Purchased','|',purchased_by_id)
            ELSE CONCAT('Transferred','|',transferred_from_id) 
        END AS ticket_status_owner
        , purchased_by_id AS owner_id
        , owner.first_name AS owner_first_name
        , owner.last_name AS owner_last_name
        , owner.full_name AS owner_full_name
        , transferred_from_id
        , transfer.first_name AS transferred_first_name
        , transfer.last_name AS transferred_last_name
        , transfer.full_name AS transferred_full_name
        , transaction_date_time AS transaction_date
        , event.start_date AS sporting_event_date
        , ticket.sporting_event_id AS sporting_event_id
        , event.sport_type_name AS sporting_event_id
        , event.home_team_id AS home_team_id
        , home_team.name AS home_team_name
        , home_team.abbreviated_name AS home_team_abbrev
        , CONCAT(home_team.sport_league_short_name,'|',home_team.sport_division_short_name) AS home_team_sport_league
        , event.away_team_id
        , away_team.name AS away_team_name
        , away_team.abbreviated_name AS away_team_abbrev
        , CONCAT(away_team.sport_league_short_name,'|',away_team.sport_division_short_name) AS away_team_sport_league
        , location.id AS home_field_id
        , location.name AS home_field_name
        , location.city AS home_field_city
        , location.seating_capacity AS home_field_seating_capacity
        , seat_level
        , seat_section
        , seat_row
        , seat
        , purchase_price AS ticket_price
        FROM ticket_purchase_hist purchaseHistory
        LEFT JOIN person owner ON purchaseHistory.purchased_by_id = owner.ID
        LEFT JOIN person transfer ON purchaseHistory.transferred_from_id = transfer.ID
        LEFT JOIN sporting_event_ticket ticket ON purchaseHistory.sporting_event_ticket_id = ticket.id
        LEFT JOIN sporting_event event ON ticket.sporting_event_id = event.id
        LEFT JOIN sport_location location ON event.location_id = location.id
        LEFT JOIN sport_team home_team ON event.home_team_id = home_team.id
        LEFT JOIN sport_team away_team ON event.away_team_id = away_team.id  
        """)

        writeToDynamoDB("ticket_sales", ticketDF, sc);
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