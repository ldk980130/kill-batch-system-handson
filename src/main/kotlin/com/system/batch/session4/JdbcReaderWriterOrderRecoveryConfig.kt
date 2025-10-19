package com.system.batch.session4

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PostConstruct
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.database.JdbcBatchItemWriter
import org.springframework.batch.item.database.JdbcPagingItemReader
import org.springframework.batch.item.database.Order
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import java.time.LocalDateTime
import javax.sql.DataSource


/**
 * 우리의 미션은 해킹당한 쇼핑몰의 주문 시스템을 복구하는 것이다.
 * 해커들이 주문 데이터베이스를 조작하여 정상적으로 처리 완료된 주문들을 CANCELLED로 바꾸거나,
 * 배송도 안된 주문을 SHIPPED로 변경해놓는 등 시스템을 완전히 망가뜨려놓았다.
 *
 * 우리는 JDBC reader/writer를 무기로 삼아 이 혼돈을 바로잡을 것이다.💀
 */
@Configuration
class JdbcReaderWriterOrderRecoveryConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val dataSource: DataSource,
    private val jdbcTemplate: JdbcTemplate,
) {
    private val log: KLogger = KotlinLogging.logger {}

    @Bean
    fun orderRecoveryJob(): Job {
        return JobBuilder("orderRecoveryJob", jobRepository)
            .start(orderRecoveryStep())
            .build()
    }

    @Bean
    fun orderRecoveryStep(): Step {
        return StepBuilder("orderRecoveryStep", jobRepository)
            .chunk<HackedOrder, HackedOrder>(10, transactionManager)
            .reader(compromisedOrderReader())
            .processor(orderStatusProcessor())
            .writer(orderStatusWriter())
            .build()
    }


    @Bean
    fun orderStatusWriter(): JdbcBatchItemWriter<HackedOrder> =
        JdbcBatchItemWriterBuilder<HackedOrder>()
            .dataSource(dataSource)
            .sql("UPDATE orders SET status = :status WHERE id = :id")
            .beanMapped()
            .assertUpdates(true) // fase라면 일부 데이터가 업데이트되지 않아도 계속 진행, true면 예외를 던져 배치 중단
            .build()

    @Bean
    fun orderStatusProcessor(): ItemProcessor<HackedOrder, HackedOrder> =
        ItemProcessor { order ->
            if (order.shippingId == null) {
                order.copy(status = "READY_FOR_SHIPMENT")
            } else {
                order.copy(status = "SHIPPED")
            }
        }

    @Bean
    fun compromisedOrderReader(): JdbcPagingItemReader<HackedOrder> =
        JdbcPagingItemReaderBuilder<HackedOrder>()
            .name("compromisedOrderReader")
            .dataSource(dataSource)
            .pageSize(10)
            .selectClause(
                "SELECT id, customer_id, order_datetime, status, shipping_id"
            )
            .fromClause("FROM orders")
            .whereClause(
                """
                    WHERE (status = 'SHIPPED' AND shipping_id IS NULL) OR
                    (status = 'CANCELLED' AND shipping_id IS NOT NULL)
                    """.trimMargin()
            )
            .sortKeys(mapOf("id" to Order.ASCENDING))
            .dataRowMapper(HackedOrder::class.java)
            .build()

    @PostConstruct
    fun init() {
        val createTableSql =
            """
            CREATE TABLE IF NOT EXISTS orders (
                id BIGSERIAL PRIMARY KEY,
                customer_id BIGINT NOT NULL,
                order_datetime TIMESTAMP NOT NULL,
                status VARCHAR(20) NOT NULL,
                shipping_id VARCHAR(50)
            )
            """.trimIndent()

        jdbcTemplate.execute(createTableSql)
        log.info { "Orders table checked/created successfully" }

        val count =
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM orders",
                Int::class.java,
            ) ?: 0

        if (count == 0) {
            val insertSql =
                """
                INSERT INTO orders (customer_id, order_datetime, status, shipping_id)
                SELECT
                    floor(random() * 100 + 1),
                    NOW() - (random() * INTERVAL '30 days'),
                    CASE
                        WHEN rn <= 10 THEN 'READY_FOR_SHIPMENT'
                        WHEN rn <= 20 THEN 'SHIPPED'
                        ELSE 'CANCELLED'
                    END,
                    CASE
                        WHEN rn <= 10 THEN NULL
                        WHEN rn <= 20 THEN NULL
                        ELSE 'SHIP-' || LPAD(CAST(rn AS VARCHAR), 8, '0')
                    END
                FROM (
                    SELECT GENERATE_SERIES(1, 30) AS rn
                ) AS series
                """.trimIndent()

            jdbcTemplate.execute(insertSql)
            log.info { "Sample data inserted successfully" }
        }
    }
}

data class HackedOrder(
    val id: Long,
    val customerId: Long,
    val orderDatetime: LocalDateTime,
    val status: String,
    val shippingId: String? = null,
)
