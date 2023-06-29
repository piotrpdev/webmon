package producer

import common.KafkaConfig.getCommonConfig
import common.KafkaConfig.topicName
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.net.HttpURLConnection
import java.net.URL

object Producer {
    val producer: Producer<String, Int> = KafkaProducer(
        getCommonConfig()
    )

    fun produce(urls: List<String>) = runBlocking {
        val deferred = urls.map { url ->
            async {
                while (true) {
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

                    delay(2000)
                }
            }
        }

        deferred.awaitAll()
    }

    fun printRecord(record: ProducerRecord<String, Int>) {
        println("Sent record:")
        println("\tTopic = " + record.topic())
        println("\tPartition = " + record.partition())
        println("\tKey = " + record.key())
        println("\tValue = " + record.value())
    }
}