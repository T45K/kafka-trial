import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

private const val KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

suspend fun main() = coroutineScope {
    val producerProps = mapOf(
        "bootstrap.servers" to KAFKA_BOOTSTRAP_SERVER,
        "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
        "security.protocol" to "PLAINTEXT"
    )

    launch {
        generateSequence(1) { it + 1 }.forEach { count ->
            KafkaProducer<String, ByteArray>(producerProps).use {
                it.asyncSend(ProducerRecord("test", "1", "Hello, world $count!".encodeToByteArray()))
            } // Message will be sent when KafkaProducer is closed
        }
    }

    val consumerProps = mapOf(
        "bootstrap.servers" to KAFKA_BOOTSTRAP_SERVER,
        "auto.offset.reset" to "earliest",
        "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "group.id" to "someGroup",
        "security.protocol" to "PLAINTEXT"
    )

    KafkaConsumer<String, ByteArray>(consumerProps).use { consumer ->
        consumer.subscribe(listOf("test"))
        while (true) {
            val message = repeatUntilSome {
                consumer.poll(400.milliseconds.toJavaDuration())
                    .map { String(it.value()) }
                    .firstOrNull()
            }
            println(message)
        }
    }
}

suspend fun <K, V> Producer<K, V>.asyncSend(record: ProducerRecord<K, V>): RecordMetadata =
    suspendCoroutine { continuation ->
        send(record) { metadata, exception ->
            exception?.let(continuation::resumeWithException)
                ?: continuation.resume(metadata)
        }
    }

tailrec fun <T> repeatUntilSome(block: () -> T?): T = block() ?: repeatUntilSome(block)
