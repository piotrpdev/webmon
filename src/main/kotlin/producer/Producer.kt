package producer

import common.KafkaConfig.getCommonConfig
import common.KafkaConfig.topicName
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.net.HttpURLConnection
import java.net.URL

object Producer {
    fun produce(urls: List<String>) = runBlocking {
        val producer: Producer<String, Int> = KafkaProducer(
            getCommonConfig()
        )

        launch {
            while (true) {
                for (url in urls) {
                    val urlObject = URL(url)
                    val http: HttpURLConnection = urlObject.openConnection() as HttpURLConnection
                    val statusCode: Int = http.responseCode

                    val record = ProducerRecord(
                        topicName,
                        url,
                        statusCode
                    )

                    producer.send(record)
                    printRecord(record)
                }
                delay(5000)
            }
            producer.close()
        }
    }

    fun printRecord(record: ProducerRecord<String, Int>) {
        println("Sent record:")
        println("\tTopic = " + record.topic())
        println("\tPartition = " + record.partition())
        println("\tKey = " + record.key())
        println("\tValue = " + record.value())
    }
}