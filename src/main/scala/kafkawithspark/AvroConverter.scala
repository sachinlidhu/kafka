package kafkawithspark

object AvroConverter {
  import org.apache.avro.Schema
  import org.apache.avro.generic.{GenericData, GenericRecord}
  /*import org.apache.avro.io.DatumWriter
  import org.apache.avro.io.EncoderFactory
  import org.apache.avro.specific.SpecificDatumWriter*/

  def toGenericRecord(row: org.apache.spark.sql.Row, schema: org.apache.spark.sql.types.StructType): GenericRecord = {
    val avroSchema = new Schema.Parser().parse(schema.json)
    val record = new GenericData.Record(avroSchema)
    schema.fields.zipWithIndex.foreach {
      case (field, index) =>
        val value = row.get(index)
        record.put(field.name, value)
    }
    record
  }
}

