package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.core.EventSourcingService
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.min

class RateLimitedException(val retryAfter: Long)
    : Exception("Rate limited, retry after $retryAfter seconds.")

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

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )
    private val semaphore = Semaphore(parallelRequests, true)

    private val meterRegistry: MeterRegistry = Metrics.globalRegistry
    private val semaphoreRequestsCounter: Counter
    private val semaphoreAcquiredCounter: Counter
    private val semaphoreTimeoutCounter: Counter

    init {
        semaphoreRequestsCounter = Counter.builder("payment.semaphore.requests.total")
            .description("Total number of requests to the semaphore")
            .tag("account", accountName)
            .tag("service", serviceName)
            .register(meterRegistry)

        semaphoreAcquiredCounter = Counter.builder("payment.semaphore.acquired.total")
            .description("Total number of requests that acquired the semaphore")
            .tag("account", accountName)
            .tag("service", serviceName)
            .register(meterRegistry)

        semaphoreTimeoutCounter = Counter.builder("payment.semaphore.timeouts.total")
            .description("Total number of requests that timed out waiting for semaphore")
            .tag("account", accountName)
            .tag("service", serviceName)
            .register(meterRegistry)

        meterRegistry.gauge(
            "payment.semaphore.available.permits",
            listOf(
                io.micrometer.core.instrument.Tag.of("account", accountName),
                io.micrometer.core.instrument.Tag.of("service", serviceName)
            ),
            semaphore
        ) { it.availablePermits().toDouble() }

        meterRegistry.gauge(
            "payment.semaphore.max.permits",
            listOf(
                io.micrometer.core.instrument.Tag.of("account", accountName),
                io.micrometer.core.instrument.Tag.of("service", serviceName)
            ),
            semaphore
        ) { parallelRequests.toDouble() }

        meterRegistry.gauge(
            "payment.semaphore.queue.length",
            listOf(
                io.micrometer.core.instrument.Tag.of("account", accountName),
                io.micrometer.core.instrument.Tag.of("service", serviceName)
            ),
            semaphore
        ) { it.queueLength.toDouble() }

        logger.info("Metrics initialized for account: $accountName, service: $serviceName")
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val remainingTime = deadline - now()

        val plainRateLimit = rateLimitPerSec.toLong()
        val inflightRequestRateLimit = parallelRequests * 1000 / processingTime.toMillis()
        val realRateLimit = min(plainRateLimit, inflightRequestRateLimit)
        val estimatedTimeWaiting = parallelRequests / realRateLimit * 1000

        if (remainingTime <= 0) {
            logger.warn("[$accountName] Rejecting payment $paymentId: deadline already passed")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline already passed")
            }
            return
        }

        semaphoreRequestsCounter.increment()

        if (!semaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
            logger.warn("[$accountName] Rejecting payment $paymentId: parallel requests limit reached")
            semaphoreTimeoutCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Parallel requests limit reached")
            }
            return
        }

        semaphoreAcquiredCounter.increment()

        try {
            if (!rateLimiter.tick()) {
                throw RateLimitedException(estimatedTimeWaiting)
            }

            val request = Request.Builder()
                .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } finally {
            semaphore.release()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
