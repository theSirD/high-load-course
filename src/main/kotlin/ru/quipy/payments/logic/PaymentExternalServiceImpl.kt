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
import java.util.concurrent.Executors


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
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val rateLimiterConfig = RateLimiterConfig.custom()
        .limitRefreshPeriod(Duration.ofMillis(10))
        .limitForPeriod(maxOf(1, properties.rateLimitPerSec / 100))
        .timeoutDuration(maxOf(
            properties.averageProcessingTime.multipliedBy(2),
            Duration.ofMillis(500)
        ))
        .build()

    private val rateLimiter = RateLimiterRegistry.of(rateLimiterConfig)
        .rateLimiter("payment-rate-limiter:$accountName")

    private val responseExecutor = Executors.newFixedThreadPool(128)

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

        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(5))
            .build()

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .whenCompleteAsync({ response, throwable ->
                val httpDoneAt = System.currentTimeMillis()
                val httpMs = httpDoneAt - afterLogSubmission
                if (throwable != null) {
                    val totalFromStart = httpDoneAt - paymentStartedAt
                    logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId rateLimitMs=$rateLimitMs logSubmissionMs=$logSubmissionMs httpMs=$httpMs totalFromStart=$totalFromStart success=false reason=${throwable.message ?: "Network_error"}")
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", throwable)
                    // paymentESService.update(paymentId) {
                    //     it.logProcessing(false, System.currentTimeMillis(), transactionId, reason = throwable.message ?: "Network error")
                    // }
                } else {
                    val bodyString = response.body()
                    val body = try {
                        mapper.readValue(bodyString, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: $bodyString")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    // paymentESService.update(paymentId) {
                    //     it.logProcessing(body.result, System.currentTimeMillis(), transactionId, reason = body.message)
                    // }
                    val afterLogProcessing = System.currentTimeMillis()
                    val logProcessingMs = afterLogProcessing - httpDoneAt
                    val totalFromStart = afterLogProcessing - paymentStartedAt
                    logger.info("PAYMENT_METRICS paymentId=$paymentId transactionId=$transactionId rateLimitMs=$rateLimitMs logSubmissionMs=$logSubmissionMs httpMs=$httpMs logProcessingMs=$logProcessingMs totalFromStart=$totalFromStart success=${body.result}")
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                }
            }, responseExecutor)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}
