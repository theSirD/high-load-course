package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import io.micrometer.core.instrument.Metrics
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
        private const val HEDGE_DELAY_STAGGER_MS = 5L
        private const val MIN_REMAINING_FOR_HEDGE_MS = 150L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val rateLimiterConfig = RateLimiterConfig.custom()
        .limitRefreshPeriod(Duration.ofMillis(10))
        .limitForPeriod(maxOf(1, properties.rateLimitPerSec / 100))
        .timeoutDuration(Duration.ofMillis(500))
        .build()

    private val rateLimiter = RateLimiterRegistry.of(rateLimiterConfig)
        .rateLimiter("payment-rate-limiter:$accountName")

    private val responseExecutor = Executors.newFixedThreadPool(128)

    private val hedgeScheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(2)

    private val semaphore = Semaphore(properties.parallelRequests)

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .executor(responseExecutor)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val t0 = System.currentTimeMillis()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        try {
            rateLimiter.acquirePermission()
        } catch (e: io.github.resilience4j.ratelimiter.RequestNotPermitted) {
            val t = System.currentTimeMillis()
            val rateLimitMs = t - t0
            val totalFromStart = t - paymentStartedAt
            logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId rateLimitMs=$rateLimitMs totalFromStart=$totalFromStart success=false reason=Rate_limit_timeout")
            logger.warn("[$accountName] Rate limit timeout for payment $paymentId")
            // paymentESService.update(paymentId) {
            //     it.logSubmission(success = false, transactionId, t, Duration.ofMillis(t - paymentStartedAt))
            //     it.logProcessing(false, t, transactionId, reason = "Rate limit timeout")
            // }
            return
        }

        val afterRateLimit = System.currentTimeMillis()
        val rateLimitMs = afterRateLimit - t0
        // paymentESService.update(paymentId) {
        //     it.logSubmission(success = true, transactionId, afterRateLimit, Duration.ofMillis(afterRateLimit - paymentStartedAt))
        // }

        val afterLogSubmission = System.currentTimeMillis()
        val logSubmissionMs = afterLogSubmission - afterRateLimit
        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val remainingMs = deadline - System.currentTimeMillis()
        val deadlineWindowMs = deadline - paymentStartedAt
        val hedgeCount = if (properties.averageProcessingTime.toMillis() >= deadlineWindowMs * 0.9) {
            when {
                remainingMs >= (deadlineWindowMs * 0.90).toLong() -> 4
                remainingMs >= (deadlineWindowMs * 0.70).toLong() -> 6
                else -> 8
            }
        } else 1
        val permits = hedgeCount

        val minRequiredForHttpMs = maxOf(150L, properties.averageProcessingTime.toMillis() / 2)
        if (remainingMs < minRequiredForHttpMs) {
            val t = System.currentTimeMillis()
            logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId " +
                "rateLimitMs=$rateLimitMs logSubmissionMs=0 httpMs=0 totalFromStart=${t - paymentStartedAt} " +
                "success=false reason=Deadline_too_short_for_http")
            return
        }
        if (remainingMs <= 0 || !semaphore.tryAcquire(permits, remainingMs, TimeUnit.MILLISECONDS)) {
            val t = System.currentTimeMillis()
            logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId " +
                "rateLimitMs=$rateLimitMs logSubmissionMs=0 httpMs=0 totalFromStart=${t - paymentStartedAt} " +
                "success=false reason=Semaphore_timeout")
            return
        }

        val remainingForHttp = deadline - System.currentTimeMillis()
        if (remainingForHttp < MIN_REMAINING_FOR_HEDGE_MS) {
            semaphore.release(permits)
            val t = System.currentTimeMillis()
            logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId " +
                "rateLimitMs=$rateLimitMs logSubmissionMs=${t - afterLogSubmission} httpMs=0 " +
                "totalFromStart=${t - paymentStartedAt} success=false reason=Deadline_too_close")
            return
        }

        val failuresRemaining = AtomicInteger(hedgeCount)
        val result = CompletableFuture<HttpResponse<String>>()
        result.whenCompleteAsync({ response, throwable ->
            val httpDoneAt = System.currentTimeMillis()
            val httpMs = httpDoneAt - afterLogSubmission
            if (throwable != null) {
                val totalFromStart = httpDoneAt - paymentStartedAt
                logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId rateLimitMs=$rateLimitMs logSubmissionMs=$logSubmissionMs httpMs=$httpMs totalFromStart=$totalFromStart success=false reason=${throwable.message ?: "Network_error"}")
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", throwable)
                semaphore.release(permits)
            } else {
                val bodyString = response!!.body()
                val body = try {
                    mapper.readValue(bodyString, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: $bodyString")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                val afterLogProcessing = System.currentTimeMillis()
                val logProcessingMs = afterLogProcessing - httpDoneAt
                val totalFromStart = afterLogProcessing - paymentStartedAt
                logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId rateLimitMs=$rateLimitMs logSubmissionMs=$logSubmissionMs httpMs=$httpMs logProcessingMs=$logProcessingMs totalFromStart=$totalFromStart success=${body.result}")
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                semaphore.release(permits)
            }
        }, responseExecutor)

        fun sendOneRequest(remainingForHttpNow: Long, txId: UUID) {
            val httpTimeoutMs = minOf(5000L, remainingForHttpNow - 5)
            val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$txId&paymentId=$paymentId&amount=$amount"
            val request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofMillis(httpTimeoutMs))
                .build()
            val future = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            future.whenCompleteAsync({ r, t ->
                if (result.isDone) return@whenCompleteAsync
                if (t != null) {
                    if (failuresRemaining.decrementAndGet() == 0) {
                        result.completeExceptionally(t)
                    }
                } else {
                    r?.let { result.complete(it) }
                }
            }, responseExecutor)
        }

        for (i in 0 until hedgeCount) {
            if (i == 0) {
                sendOneRequest(remainingForHttp, transactionId)
            } else {
                hedgeScheduler.schedule({
                    if (result.isDone) return@schedule
                    val remainingForHttp2 = deadline - System.currentTimeMillis()
                    if (remainingForHttp2 < MIN_REMAINING_FOR_HEDGE_MS) {
                        if (failuresRemaining.decrementAndGet() == 0 && !result.isDone) {
                            result.completeExceptionally(Exception("Hedge deadline too close"))
                        }
                        return@schedule
                    }
                    val txId = UUID.randomUUID()
                    sendOneRequest(remainingForHttp2, txId)
                }, i * HEDGE_DELAY_STAGGER_MS, TimeUnit.MILLISECONDS)
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun rateLimitPerSec() = properties.rateLimitPerSec

}
