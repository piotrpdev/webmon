package consumer

import common.KafkaConfig.connectionConfig
import common.KafkaConfig.getCommonConfig
import common.KafkaConfig.topicName
import consumer.model.WebsiteStatus
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.sql.DriverManager
import java.sql.SQLException
import java.time.Duration
import java.time.OffsetDateTime


object Consumer {
    val kafkaConsumer = KafkaConsumer<String, String>(getCommonConfig())
    var consumerIsAlive = true
    var latestInDbTime: String? = null
    var latestInDbFoundInTopic = false
    var preConsumerDbCount: Int? = null

    internal fun createWebsiteStatusTable() {
        val SQL_CREATE = """
            CREATE TABLE IF NOT EXISTS public."website-status" (
                id SERIAL PRIMARY KEY,
                url TEXT NOT NULL,
                http_status INT NOT NULL,
                recorded_at TEXT NOT NULL
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

    fun selectCount(): Int? {
        var result: Int? = null

        val SQL_SELECT = "SELECT COUNT(*) FROM public.\"website-status\""

        try {
            DriverManager.getConnection(
                connectionConfig.postgresAddress, connectionConfig.postgresUsername, connectionConfig.postgresPassword
            ).use { conn ->
                conn.prepareStatement(SQL_SELECT).use { preparedStatement ->
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        result = resultSet.getInt("count")
                    }
                }
            }
        } catch (e: SQLException) {
            System.err.format("SQL State: %s\n%s", e.sqlState, e.message)
        } catch (e: Exception) {
            e.printStackTrace()
        }

        return result
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
                        val recorded_at = resultSet.getObject("recorded_at", OffsetDateTime::class.java)
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

    fun selectLatest(): WebsiteStatus? {
        var result: WebsiteStatus? = null

        val SQL_SELECT = "SELECT * FROM public.\"website-status\" ORDER BY id DESC LIMIT 1"

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
                        val recorded_at = resultSet.getObject("recorded_at", OffsetDateTime::class.java)
                        val obj = WebsiteStatus(id, url, http_status, recorded_at)
                        result = obj
                    }
                }
            }
        } catch (e: SQLException) {
            System.err.format("SQL State: %s\n%s", e.sqlState, e.message)
        } catch (e: Exception) {
            e.printStackTrace()
        }

        return result
    }

    fun insertWebsiteStatus(url: String, http_status: Int, recorded_at: OffsetDateTime) {
        // https://www.postgresqltutorial.com/postgresql-jdbc/insert/
        val SQL_INSERT = "INSERT INTO public.\"website-status\"(url, http_status, recorded_at) VALUES (?, ?, ?)"

        try {
            DriverManager.getConnection(
                connectionConfig.postgresAddress, connectionConfig.postgresUsername, connectionConfig.postgresPassword
            ).use { conn ->
                conn.prepareStatement(SQL_INSERT).use { preparedStatement ->
                    preparedStatement.setString(1, url)
                    preparedStatement.setInt(2, http_status)
                    preparedStatement.setObject(3, recorded_at)
                    preparedStatement.executeUpdate()
                }
            }
        } catch (e: SQLException) {
            System.err.format("SQL State: %s\n%s", e.sqlState, e.message)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun insertManyWebsiteStatuses(websiteStatusBatch: MutableList<WebsiteStatus>) {
        // https://www.postgresqltutorial.com/postgresql-jdbc/insert/
        val SQL_INSERT = "INSERT INTO public.\"website-status\"(url, http_status, recorded_at) VALUES (?, ?, ?)"

        try {
            DriverManager.getConnection(
                connectionConfig.postgresAddress, connectionConfig.postgresUsername, connectionConfig.postgresPassword
            ).use { conn ->
                conn.prepareStatement(SQL_INSERT).use { preparedStatement ->
                    var count = 0;
                    websiteStatusBatch.forEach { (_, url, http_status, recorded_at) ->
                        preparedStatement.setString(1, url)
                        preparedStatement.setInt(2, http_status)
                        preparedStatement.setObject(3, recorded_at)
                        preparedStatement.addBatch()
                        count++

                        // execute every 100 rows or less
                        if (count % 100 == 0 || count == websiteStatusBatch.size) {
                            println("Executing batch of $count rows")
                            preparedStatement.executeBatch();
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            System.err.format("SQL State: %s\n%s", e.sqlState, e.message)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    fun consumeKafkaLoop() {
        val spaceRegex = "\\s+".toRegex()
        kafkaConsumer.subscribe(listOf(topicName))

        while (consumerIsAlive) {
            println("Waiting for events...")
            val records = kafkaConsumer.poll(Duration.ofMillis(3000))
            val recordsCount = records.count()

            if (recordsCount == 0) {
                println("No records found")
                continue
            }

            val websiteStatusBatch = mutableListOf<WebsiteStatus>()

            println("Got $recordsCount records")
            for (record in records) {
                val recordSplit = record.value().split(spaceRegex)
                val recordTime = OffsetDateTime.parse(recordSplit[0])
                val recordStatus = recordSplit[1].toInt()

                println("Status for '${record.key()}' in partition ${record.partition()} = " + record.value())

                if (latestInDbFoundInTopic) {
                    websiteStatusBatch.add(WebsiteStatus(-1, record.key(), recordStatus, recordTime))
                }

                if (!latestInDbFoundInTopic && latestInDbTime != null && recordTime.toString() == latestInDbTime) {
                    println("Lastest DB record found in topic! Adding records to DB from now on")
                    latestInDbFoundInTopic = true
                }
            }

            if (websiteStatusBatch.isNotEmpty()) {
                println("Trying to insert ${websiteStatusBatch.size} records to DB")
                insertManyWebsiteStatuses(websiteStatusBatch)
            }
        }
    }

    fun consume() {
//        val offsetNow = OffsetDateTime.parse("2023-07-04T13:12:33.729338Z")
//        println("Offset now: $offsetNow")
//        insertWebsiteStatus("http://httpd-2-website-monitor.apps.eu46a.prod.ole.redhat.com'", 200, offsetNow)
//        createWebsiteStatusTable()
        preConsumerDbCount = selectCount()
        println("Pre-consumer DB count: $preConsumerDbCount")
        if (preConsumerDbCount == 0) {
            println("No records in DB, consuming all records from Kafka")
            latestInDbFoundInTopic = true
        } else {
            println("Records found in DB, consuming only new records from Kafka")
            latestInDbTime = selectLatest()?.recorded_at.toString()
            println("Latest time in DB: $latestInDbTime")
        }

        consumeKafkaLoop()
    }
}