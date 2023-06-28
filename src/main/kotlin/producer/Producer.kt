package producer

import common.KafkaConfig.getCommonConfig
import common.KafkaConfig.topicName
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

object Producer {
    fun produce() {
        val random = Random()
        val producer: Producer<Void, Int> = KafkaProducer(
            getCommonConfig()
        )
        for (i in 0..9) {
            val record = ProducerRecord<Void, Int>(
                topicName,
                random.nextInt(100)
            )
            producer.send(record)
            printRecord(record)
        }
        producer.close()
    }

    fun printRecord(record: ProducerRecord<Void, Int>) {
        println("Sent record:")
        println("\tTopic = " + record.topic())
        println("\tPartition = " + record.partition())
        println("\tKey = " + record.key())
        println("\tValue = " + record.value())
    }
}