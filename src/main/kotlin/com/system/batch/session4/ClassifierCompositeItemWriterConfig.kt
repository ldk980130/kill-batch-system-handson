package com.system.batch.session4

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.support.ClassifierCompositeItemWriter
import org.springframework.batch.item.support.ListItemReader
import org.springframework.classify.Classifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class ClassifierCompositeItemWriterConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    private val log: KLogger = KotlinLogging.logger {}

    @Bean
    fun systemLogProcessingJob(): Job =
        JobBuilder("systemLogProcessingJob", jobRepository)
            .start(systemLogProcessingStep())
            .build()

    @Bean
    fun systemLogProcessingStep(): Step =
        StepBuilder("systemLogProcessingStep", jobRepository)
            .chunk<SystemLog, SystemLog>(10, transactionManager)
            .reader(systemLogProcessingReader())
            .writer(classifierWriter())
            .build()

    @Bean
    fun systemLogProcessingReader(): ListItemReader<SystemLog> {
        val logs = mutableListOf<SystemLog>()

        // 테스트용 데이터 생성
        val criticalLog =
            SystemLog(
                type = "CRITICAL",
                message = "OOM 발생!! 메모리가 바닥났다!",
                cpuUsage = 95,
                memoryUsage = 2024L * 1024 * 1024,
            )
        logs.add(criticalLog)

        val normalLog =
            SystemLog(
                type = "NORMAL",
                message = "시스템 정상 작동 중",
                cpuUsage = 30,
                memoryUsage = 512L * 1024 * 1024,
            )
        logs.add(normalLog)

        return ListItemReader(logs)
    }

    @Bean
    fun classifierWriter(): ClassifierCompositeItemWriter<SystemLog> =
        ClassifierCompositeItemWriter<SystemLog>()
            .apply {
                setClassifier(
                    SystemLogClassifier(
                        criticalLogWriter(),
                        normalLogWriter(),
                    ),
                )
            }

    @Bean
    fun normalLogWriter(): ItemWriter<SystemLog> =
        ItemWriter { items ->
            log.info { "✅NormalLogWriter: 일반 로그 처리 중... 대충 파일에 출력하거나 하자.." }
            items.forEach { item ->
                log.info { "✅일반 처리: $item" }
            }
        }

    @Bean
    fun criticalLogWriter(): ItemWriter<SystemLog> =
        ItemWriter { items ->
            log.info { "🚨CriticalLogWriter: 치명적 시스템 로그 감지! 즉시 처리 시작!" }
            items.forEach { item ->
                // 실제 운영에선 여기서 슬랙 혹은 이메일 발송
                log.info { "🚨긴급 처리: $item" }
            }
        }

    class SystemLogClassifier(
        private val criticalWriter: ItemWriter<SystemLog>,
        private val normalWriter: ItemWriter<SystemLog>,
    ) : Classifier<SystemLog, ItemWriter<in SystemLog>> {
        companion object {
            const val CRITICAL_CPU_THRESHOLD = 90
            const val CRITICAL_MEMORY_THRESHOLD = 1024L * 1024 * 1024 // 1GB
        }

        override fun classify(log: SystemLog): ItemWriter<in SystemLog> =
            if (isCritical(log)) {
                criticalWriter
            } else {
                normalWriter
            }

        private fun isCritical(log: SystemLog): Boolean =
            log.type == "CRITICAL" ||
                log.cpuUsage >= CRITICAL_CPU_THRESHOLD ||
                log.memoryUsage >= CRITICAL_MEMORY_THRESHOLD
    }

    data class SystemLog(
        var type: String = "", // CRITICAL or NORMAL
        var message: String = "",
        var cpuUsage: Int = 0,
        var memoryUsage: Long = 0L,
    )
}
