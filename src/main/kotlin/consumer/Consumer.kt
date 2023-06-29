package consumer

import common.KafkaConfig.getCommonConfig
import common.KafkaConfig.topicName
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

object Consumer {
    fun consume() {
        val consumer: Consumer<Void, Int> =
            KafkaConsumer<Void, Int>(getCommonConfig())

        consumer.subscribe(listOf(topicName))

        while (true) {
            println("Waiting for events...")
            val records = consumer.poll(Duration.ofMillis(10000))
            for (record in records) {
                println("Status for '${record.key()}' = " + record.value())
            }
        }
    }
}