/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql._
import org.apache.kudu.spark.kudu.KuduContext
import java.sql.Timestamp

import id.dei.TelegramMessage

case class KafkaMessage(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Long)

case class TelegramMessageX(messageid: String, chatid: String, message: String, username: String, viewtime: Long)

object SimpleApp {

  def deserialize(message: KafkaMessage): TelegramMessageX = {
    val reader = new SpecificDatumReader[TelegramMessage](TelegramMessage.getClassSchema)
    val decoder = DecoderFactory.get.binaryDecoder(message.value, null)
    val msg = reader.read(null, decoder)

    return TelegramMessageX(messageid=msg.getMessageId.toString, chatid=msg.getChatId.toString, message=msg.getMessage.toString, username=msg.getUsername.toString, viewtime=msg.getViewtime)
  }

  def main(args: Array[String]) {
    val servers = "localhost:9092"

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", "telegram")
      .load()


    import spark.implicits._

    val ds = df.as[KafkaMessage].filter(x => x.value != null).map(x => deserialize(x))

    val query = ds.writeStream
      .format("console")
      .start()

    val kuduContext = new KuduContext("localhost:7051", spark.sparkContext)

    ds.writeStream.foreachBatch { (batchDF, batchId) =>
       kuduContext.insertRows(batchDF.toDF, "telegrams")
    }.start()

    query.awaitTermination()
  }
}
