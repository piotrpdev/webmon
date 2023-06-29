@file:Suppress("INLINE_FROM_HIGHER_PLATFORM")
package common

import com.sksamuel.hoplite.ConfigLoader
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import java.util.*

data class KafkaConnectionConfig(
    val bootstrapServers: String,
    val sslTruststoreLocation: String,
    val sslTruststorePassword: String,
    val sslCaLocation: String,
    val sslCertificateLocation: String,
)

object KafkaConfig {
    val topicName = "website-status"
    val connectionConfig = ConfigLoader().loadConfigOrThrow<KafkaConnectionConfig>("/kafka_config.yaml")

    fun getCommonConfig(): Properties {
        val props = Properties()

        // TODO: Move all of these to the config file
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = connectionConfig.bootstrapServers

        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"

        props[ConsumerConfig.GROUP_ID_CONFIG] = "$topicName-consumer-group"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SSL"
        props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = connectionConfig.sslTruststoreLocation
        props[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = connectionConfig.sslTruststorePassword
        return props
    }
}