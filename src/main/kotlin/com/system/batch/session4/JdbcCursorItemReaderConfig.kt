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
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
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

    @Bean
    fun terminatedVictimReader(): JdbcCursorItemReader<Victim> =
        JdbcCursorItemReaderBuilder<Victim>()
            .name("terminatedVictimReader")
            .dataSource(dataSource)
            .sql("SELECT * FROM victims WHERE status = ? AND terminated_at <= ?")
            .queryArguments(listOf("TERMINATED", LocalDateTime.now()))
            .dataRowMapper(Victim::class.java)
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
                ('infinite_loop', 'PID_24680', '2024-02-15 18:45:00', 'RUNNING')
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
