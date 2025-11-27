package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import java.io.IOException
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val processingTime = properties.averageProcessingTime

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val client = OkHttpClient.Builder()
        .callTimeout(30, TimeUnit.SECONDS)
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(35, TimeUnit.SECONDS)
        .writeTimeout(5, TimeUnit.SECONDS)
        .dispatcher(Dispatcher().apply {
            maxRequests = 1500
            maxRequestsPerHost = 1500
        })
        .connectionPool(ConnectionPool(
            maxIdleConnections = 200,
            keepAliveDuration = 5,
            timeUnit = TimeUnit.MINUTES
        ))
        .build()

    // Используем глобальный MeterRegistry
    private val meterRegistry: MeterRegistry = Metrics.globalRegistry

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Проверка дедлайна перед отправкой запроса
        if (now() + processingTime.toMillis() > deadline) {
            logger.error("[$accountName] too late for this payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "deadline exceeded")
            }
            return
        }

        // Неблокирующий rate limit check
        if (!rateLimiter.tick()) {
            logger.warn("[$accountName] Rate limit exceeded for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "rate limit exceeded")
            }
            return
        }

        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        val start = now()

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    val requestTime = Duration.ofMillis(now() - start)
                    registerRequestTime(requestTime)

                    var body: ExternalSysResponse? = null
                    var message: String

                    try {
                        body = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        message = body.message ?: "message is null"
                        logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}", e)
                        body = ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        message = "exception cause: ${e.message}"
                    }

                    meterRegistry.counter(
                        "payment.requests.retry.message",
                        Tags.of(
                            "account", accountName,
                            "service", serviceName,
                            "transactionId", transactionId.toString(),
                            "message", message
                        )
                    ).increment()

                    // Обновляем состояние оплаты в зависимости от результата
                    paymentESService.update(paymentId) {
                        it.logProcessing(body?.result ?: false, now(), transactionId, reason = body?.message)
                    }
                } finally {
                    response.close()
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                val requestTime = Duration.ofMillis(now() - start)
                registerRequestTime(requestTime)

                val message = "network error: ${e.message}"
                logger.warn("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, message: $message", e)

                meterRegistry.counter(
                    "payment.requests.retry.message",
                    Tags.of(
                        "account", accountName,
                        "service", serviceName,
                        "transactionId", transactionId.toString(),
                        "message", message
                    )
                ).increment()

                // Обновляем состояние оплаты при ошибке
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = message)
                }
            }
        })
    }

    override fun approximateWaitingTime(queueLength: Long): Long {
        return queueLength / rateLimitPerSec * 1000 + processingTime.toMillis()
    }

    override fun getRateLimit(): Long {
        return rateLimitPerSec.toLong()
    }

    override fun getProcessingTime(): Duration {
       return processingTime
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    fun registerRequestTime(timeTaken: Duration) {
        meterRegistry.counter(
            "payment.requests.time",
            Tags.of(
                "account", accountName,
                "service", serviceName,
                "time", timeTaken.toSeconds().toString(),
            )
        ).increment()
    }
}

fun now() = System.currentTimeMillis()