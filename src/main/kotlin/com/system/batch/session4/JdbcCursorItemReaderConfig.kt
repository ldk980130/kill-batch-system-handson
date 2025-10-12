package com.system.batch.session4

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PostConstruct
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.database.JdbcPagingItemReader
import org.springframework.batch.item.database.Order
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import java.time.LocalDateTime
import javax.sql.DataSource

@Configuration
class JdbcCursorItemReaderConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val dataSource: DataSource,
    private val jdbcTemplate: JdbcTemplate,
) {
    private val log: KLogger = KotlinLogging.logger {}

    @Bean
    fun processVictimJob(): Job =
        JobBuilder("victimRecordJob", jobRepository)
            .start(processVictimStep())
            .build()

    @Bean
    fun processVictimStep(): Step =
        StepBuilder("victimRecordStep", jobRepository)
            .chunk<Victim, Victim>(5, transactionManager)
            .reader(terminatedVictimReader())
            .writer(victimWriter())
            .build()

//    @Bean
//    fun terminatedVictimReader(): JdbcCursorItemReader<Victim> =
//        JdbcCursorItemReaderBuilder<Victim>()
//            .name("terminatedVictimReader")
//            .dataSource(dataSource)
//            .sql("SELECT * FROM victims WHERE status = ? AND terminated_at <= ?")
//            .queryArguments(listOf("TERMINATED", LocalDateTime.now()))
//            .dataRowMapper(Victim::class.java)
//            .build()

    @Bean
    fun terminatedVictimReader(): JdbcPagingItemReader<Victim> =
        JdbcPagingItemReaderBuilder<Victim>()
            .name("terminatedVictimReader")
            .dataSource(dataSource)
            .pageSize(5)
            .selectClause("SELECT id, name, process_id, terminated_at, status")
            .fromClause("FROM victims")
            .whereClause("WHERE status = :status AND terminated_at <= :terminatedAt")
            .sortKeys(mapOf("id" to Order.ASCENDING))
            .parameterValues(
                mapOf(
                    "status" to "TERMINATED",
                    "terminatedAt" to LocalDateTime.now(),
                ),
            ).dataRowMapper(Victim::class.java)
            .build()

    @Bean
    fun victimWriter(): ItemWriter<Victim> =
        ItemWriter { items: Chunk<out Victim> ->
            for (victim in items) {
                log.info { "victim: $victim" }
            }
        }

    @PostConstruct
    fun init() {
        val createTableSql =
            """
            CREATE TABLE IF NOT EXISTS victims (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255),
                process_id VARCHAR(50),
                terminated_at TIMESTAMP,
                status VARCHAR(20)
            )
            """.trimIndent()

        jdbcTemplate.execute(createTableSql)
        log.info { "Victims table checked/created successfully" }

        val count =
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM victims",
                Int::class.java,
            ) ?: 0

        if (count == 0) {
            val insertSql =
                """
                INSERT INTO victims (name, process_id, terminated_at, status) VALUES
                    ('zombie_process', 'PID_12345', '2024-01-01 12:00:00', 'TERMINATED'),
                    ('sleeping_thread', 'PID_45678', '2024-01-15 15:30:00', 'TERMINATED'),
                    ('memory_leak', 'PID_98765', '2024-02-01 09:15:00', 'RUNNING'),
                    ('infinite_loop', 'PID_24680', '2024-02-15 18:45:00', 'RUNNING'),
                    ('deadlock_case', 'PID_13579', '2024-03-01 22:10:00', 'TERMINATED'),
                    ('orphan_thread', 'PID_11223', '2024-03-10 11:05:00', 'TERMINATED'),
                    ('cpu_hog', 'PID_44556', '2024-04-05 08:20:00', 'RUNNING'),
                    ('disk_io_wait', 'PID_77889', '2024-04-15 14:40:00', 'RUNNING'),
                    ('hung_service', 'PID_99001', '2024-05-01 07:30:00', 'TERMINATED'),
                    ('socket_leak', 'PID_22334', '2024-05-20 17:25:00', 'TERMINATED'),
                    ('abandoned_mutex', 'PID_55667', '2024-06-05 19:00:00', 'RUNNING'),
                    ('stuck_queue', 'PID_88990', '2024-06-18 10:10:00', 'RUNNING'),
                    ('segfault', 'PID_10293', '2024-07-03 13:50:00', 'TERMINATED'),
                    ('oom_killer', 'PID_74839', '2024-07-15 16:45:00', 'TERMINATED'),
                    ('race_condition', 'PID_56172', '2024-08-01 12:05:00', 'RUNNING'),
                    ('dead_task', 'PID_32459', '2024-08-10 21:20:00', 'TERMINATED'),
                    ('thread_pool_exhaustion', 'PID_95126', '2024-09-01 23:55:00', 'RUNNING'),
                    ('database_lock', 'PID_68542', '2024-09-15 06:35:00', 'RUNNING'),
                    ('unhandled_exception', 'PID_29741', '2024-10-01 04:25:00', 'TERMINATED'),
                    ('corrupt_memory', 'PID_14378', '2024-10-20 15:55:00', 'TERMINATED');
                """.trimIndent()

            jdbcTemplate.execute(insertSql)
            log.info { "Sample data inserted successfully" }
        }
    }
}

data class Victim(
    private val id: Long,
    private val name: String,
    private val processId: String,
    private val terminatedAt: LocalDateTime,
    private val status: String,
)
