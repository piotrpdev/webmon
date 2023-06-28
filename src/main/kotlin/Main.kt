import consumer.Consumer.consume
import kotlinx.cli.*
import producer.Producer.produce

@OptIn(ExperimentalCli::class)
fun main(args: Array<String>) {
    val parser = ArgParser("webmon")

    class Produce: Subcommand("produce", "Produce records to Kafka") {
        val url_list by argument(ArgType.String, description = "Space seperated list of URLs to monitor").vararg()

        override fun execute() {}
    }

    class Consume: Subcommand("consume", "Consume records from Kafka") {
        val url_list by argument(ArgType.String, description = "Space seperated list of URLs to monitor").vararg()

        override fun execute() {}
    }

    val produceArgs = Produce()
    val consumeArgs = Consume()

    parser.subcommands(produceArgs, consumeArgs)
    parser.parse(args)

    if (produceArgs.url_list.isNotEmpty() && consumeArgs.url_list.isNotEmpty()) {
        println("Both produce and consume arguments are provided. Please provide only one of them.")
        return
    }

    if (produceArgs.url_list.isEmpty() && consumeArgs.url_list.isEmpty()) {
        println("Please provide either produce or consume arguments.")
        return
    }

    if (produceArgs.url_list.isNotEmpty()) {
        produce()
    } else {
        consume()
    }

}