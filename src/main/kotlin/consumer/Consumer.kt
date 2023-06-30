package consumer

import common.KafkaConfig.connectionConfig
import common.KafkaConfig.getCommonConfig
import common.KafkaConfig.topicName
import consumer.model.WebsiteStatus
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant


object Consumer {
    val consumer = KafkaConsumer<String, String>(getCommonConfig())
    var consumerIsAlive = true

    internal fun createWebsiteStatusTable() {
        val SQL_CREATE = """
            CREATE TABLE IF NOT EXISTS public."website-status" (
                id SERIAL PRIMARY KEY,
                url TEXT NOT NULL,
                http_status INT NOT NULL,
                recorded_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
            );
        """.trimIndent()

        try {
            DriverManager.getConnection(
                connectionConfig.postgresAddress, connectionConfig.postgresUsername, connectionConfig.postgresPassword
            ).use { conn ->
                conn.prepareStatement(SQL_CREATE).use { preparedStatement ->
                    preparedStatement.executeUpdate()
                }
            }
        } catch (e: SQLException) {
            println(e.message)
        }
    }

    fun selectAll() {
        // https://mkyong.com/jdbc/how-do-connect-to-postgresql-with-jdbc-driver-java/
        val result: MutableList<WebsiteStatus> = ArrayList<WebsiteStatus>()

        val SQL_SELECT = "Select * from public.\"website-status\""

        try {
            DriverManager.getConnection(
                connectionConfig.postgresAddress, connectionConfig.postgresUsername, connectionConfig.postgresPassword
            ).use { conn ->
                conn.prepareStatement(SQL_SELECT).use { preparedStatement ->
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val id = resultSet.getInt("id")
                        val url = resultSet.getString("url")
                        val http_status = resultSet.getInt("http_status")
                        val recorded_at: Timestamp = resultSet.getTimestamp("recorded_at")
                        val obj = WebsiteStatus(id, url, http_status, recorded_at)
                        result.add(obj)
                    }
                    result.forEach(java.util.function.Consumer<WebsiteStatus> { x: WebsiteStatus? -> println(x) })
                }
            }
        } catch (e: SQLException) {
            System.err.format("SQL State: %s\n%s", e.sqlState, e.message)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    fun insertWebsiteStatus(url: String, http_status: Int, recorded_at: Timestamp) {
        // https://www.postgresqltutorial.com/postgresql-jdbc/insert/
        val SQL_INSERT = "INSERT INTO public.\"website-status\"(url, http_status, recorded_at) VALUES (?, ?, ?)"

        try {
            DriverManager.getConnection(
                connectionConfig.postgresAddress, connectionConfig.postgresUsername, connectionConfig.postgresPassword
            ).use { conn ->
                conn.prepareStatement(SQL_INSERT).use { preparedStatement ->
                    preparedStatement.setString(1, url)
                    preparedStatement.setInt(2, http_status)
                    preparedStatement.setTimestamp(3, recorded_at)
                    preparedStatement.executeUpdate()
                }
            }
        } catch (e: SQLException) {
            System.err.format("SQL State: %s\n%s", e.sqlState, e.message)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    fun consumeKafkaLoop() {
        val parsedTime = Timestamp.from(Instant.parse("2023-06-30T13:47:47.453207856Z"))
        insertWebsiteStatus("https://www.google.com", 200, parsedTime)
        consumer.subscribe(listOf(topicName))

        while (consumerIsAlive) {
            println("Waiting for events...")
            val records = consumer.poll(Duration.ofMillis(10000))
            println("Got ${records.count()} records")
            for (record in records) {
                println("Status for '${record.key()}' = " + record.value())
            }
        }
    }

    fun consume() {
        consumeKafkaLoop()
        //selectAll()
        //createWebsiteStatusTable()
    }
}