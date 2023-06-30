package consumer.model

import java.sql.Timestamp

data class WebsiteStatus(
    val id: Int,
    val url: String,
    val http_status: Int,
    val recorded_at: Timestamp
)
