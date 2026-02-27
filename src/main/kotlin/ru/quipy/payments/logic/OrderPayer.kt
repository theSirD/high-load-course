package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val THREADS = 128

        private const val REAL_TASK_TIME_SEC = 0.65

        private const val MIN_PROCESSING_MS = 150L
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        THREADS,
        THREADS,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(20_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(
        orderId: UUID,
        amount: Int,
        paymentId: UUID,
        deadline: Long
    ): Long {

        val createdAt = System.currentTimeMillis()

        val queueSize = paymentExecutor.queue.size

        val effectiveRps = THREADS / REAL_TASK_TIME_SEC

        val timeToStartMs = (queueSize / effectiveRps) * 1000

        val remainingTime = deadline - createdAt

        if (remainingTime < timeToStartMs + MIN_PROCESSING_MS) {

            logger.warn(
                "PAYMENT_REJECTED paymentId=$paymentId reason=Deadline_will_be_missed " +
                        "queueSize=$queueSize remainingTimeMs=$remainingTime estimatedWaitMs=$timeToStartMs"
            )

            // paymentESService.create {
            //     it.create(paymentId, orderId, amount)
            // }

            val rejectTime = System.currentTimeMillis()

            // paymentESService.update(paymentId) {
            //     it.logSubmission(
            //         false,
            //         UUID.randomUUID(),
            //         rejectTime,
            //         Duration.ofMillis(rejectTime - createdAt)
            //     )

            //     it.logProcessing(
            //         false,
            //         rejectTime,
            //         null,
            //         reason = "Deadline will be missed"
            //     )
            // }

            return createdAt
        }

        paymentExecutor.submit {

            val dequeuedAt = System.currentTimeMillis()
            val queueWaitMs = dequeuedAt - createdAt

            // val createdEvent = paymentESService.create {
            //     it.create(paymentId, orderId, amount)
            // }

            val afterCreate = System.currentTimeMillis()
            val createMs = afterCreate - dequeuedAt

            logger.info(
                "PAYMENT_METRICS paymentId=$paymentId orderId=$orderId " +
                        "queueWaitMs=$queueWaitMs createMs=$createMs"
            )

            paymentService.submitPaymentRequest(
                paymentId,
                amount,
                createdAt,
                deadline
            )
        }

        return createdAt
    }
}