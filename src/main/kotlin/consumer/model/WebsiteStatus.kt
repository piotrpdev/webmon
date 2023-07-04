package consumer.model

data class WebsiteStatus(
    val id: Int,
    val url: String,
    val http_status: Int,
    val recorded_at: String
)
