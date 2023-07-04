package consumer.model

import java.time.OffsetDateTime

data class WebsiteStatus(
    val id: Int,
    val url: String,
    val http_status: Int,
    val recorded_at: OffsetDateTime
)
