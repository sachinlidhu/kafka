package kafkawithspark

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import sampledata.SampleDataAvro
import java.time.LocalDateTime
import scala.util.Random

object DataGeneration {
  def generateOrder(): GenericData.Record = {
    val avroSchema = new Schema.Parser().parse(
      """
        |{
        |  "type": "record",
        |  "name": "Order",
        |  "fields": [
        |    {"name": "orderId", "type": "int"},
        |    {"name": "orderProductName", "type": "string"},
        |    {"name": "orderCardType", "type": "string"},
        |    {"name": "orderAmount", "type": "double"},
        |    {"name": "orderDatetime", "type": "string"},
        |    {"name": "orderCityName", "type": "string"},
        |    {"name": "orderCountryName", "type": "string"},
        |    {"name": "orderEcommerceWebsiteName", "type": "string"}
        |  ]
        |}
        |""".stripMargin
    )

    val orderId = Random.nextInt(100000) + 1
    val orderProductName = SampleDataAvro.productNames(Random.nextInt(SampleDataAvro.productNames.length))
    val orderCardType = SampleDataAvro.cardTypes(Random.nextInt(SampleDataAvro.cardTypes.length))
    val orderAmount = BigDecimal(Random.nextDouble() * 550).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val orderDatetime = LocalDateTime.now().toString
    val (orderCityName, orderCountryName) = {
      val split = SampleDataAvro.countryCityList(Random.nextInt(SampleDataAvro.countryCityList.length)).split(",")
      (split.head, split.last)
    }
    val orderEcommerceWebsiteName = SampleDataAvro.websiteNames(Random.nextInt(SampleDataAvro.websiteNames.length))

    val order = new GenericData.Record(avroSchema)
    val orderMap = Map(
      "orderId" -> orderId,
      "orderProductName" -> orderProductName,
      "orderCardType" -> orderCardType,
      "orderAmount" -> orderAmount,
      "orderDatetime" -> orderDatetime,
      "orderCityName" -> orderCityName,
      "orderCountryName" -> orderCountryName,
      "orderEcommerceWebsiteName" -> orderEcommerceWebsiteName
    )
    orderMap.foreach{ case (key,value) => order.put(key,value)}

    order
  }

}
