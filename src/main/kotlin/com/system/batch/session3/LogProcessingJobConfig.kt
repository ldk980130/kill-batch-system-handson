package com.system.batch.session3

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.MultiResourceItemReader
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.transaction.PlatformTransactionManager
import java.time.LocalDateTime


private val HOME_DIR = System.getProperty("/Users/does/IdeaProjects/kill-batch-system-handson/src/main/resources")
private const val LOG_DIR = "collected_ecommerce_logs/"

@Configuration
class LogProcessingJobConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val objectMapper: ObjectMapper,
) {
    @Bean
    fun logProcessingJob(
        createDirectoryStep: Step,
        logCollectionStep: Step,
        logProcessingStep: Step,
    ): Job = JobBuilder("logProcessingJob", jobRepository)
        .start(createDirectoryStep)
        .next(logCollectionStep)
        .next(logProcessingStep)
        .build()

    @Bean
    fun createDirectoryStep(mkdirTasklet: SystemCommandTasklet): Step =
        StepBuilder("createDirectoryStep", jobRepository)
            .tasklet(mkdirTasklet, transactionManager)
            .build()

    @Bean
    @StepScope
    fun mkdirTasklet(
        @Value("#{jobParameters['date']}") date: String,
    ): SystemCommandTasklet =
        SystemCommandTasklet()
            .apply {
                setWorkingDirectory(HOME_DIR)
                setCommand(
                    "mkdir",
                    "-p",
                    "$LOG_DIR$date",
                    "processed_logs/$date"
                )
                setTimeout(3000)
            }

    @Bean
    fun logCollectionStep(scpTasklet: SystemCommandTasklet): Step {
        return StepBuilder("logCollectionStep", jobRepository)
            .tasklet(scpTasklet, transactionManager)
            .build()
    }

    @Bean
    @StepScope
    fun scpTasklet(
        @Value("#{jobParameters['date']}") date: String,
    ): SystemCommandTasklet =
        SystemCommandTasklet().apply {
            setWorkingDirectory(HOME_DIR)

            val command =
                listOf("localhost").joinToString(" && ") { host ->
                    "scp $host:~/ecommerce_logs/$date.log ./$LOG_DIR$date/$host.log"
                }

            setCommand("/bin/sh", "-c", command)
            setTimeout(10000)
        }

    @Bean
    fun logProcessingStep(
        multiResourceItemReader: MultiResourceItemReader<RawLogEntry>,
        logEntryProcessor: ItemProcessor<RawLogEntry, ProcessedLogEntry>,
        processedLogEntryJsonWriter: FlatFileItemWriter<ProcessedLogEntry>,
    ): Step {
        return StepBuilder("logProcessingStep", jobRepository)
            .chunk<RawLogEntry, ProcessedLogEntry>(10, transactionManager)
            .reader(multiResourceItemReader)
            .processor(logEntryProcessor)
            .writer(processedLogEntryJsonWriter)
            .build()
    }

    @Bean
    @StepScope
    fun multiResourceItemReader(
        @Value("#{jobParameters['date']}") date: String,
    ): MultiResourceItemReader<RawLogEntry> =
        MultiResourceItemReaderBuilder<RawLogEntry>()
            .resources(
                PathMatchingResourcePatternResolver()
                    .getResource("file:$HOME_DIR/$LOG_DIR$date/*.log")
            )
            .build()

    @Bean
    fun logEntryProcessor(): ItemProcessor<RawLogEntry, ProcessedLogEntry> =
        object : ItemProcessor<RawLogEntry, ProcessedLogEntry> {
            override fun process(item: RawLogEntry): ProcessedLogEntry {
                return ProcessedLogEntry(
                    dateTime = LocalDateTime.parse(item.dateTime),
                    level = LogLevel.fromString(item.level),
                    message = item.message,
                    errorCode = extractErrorCode(item.message)
                )
            }

            private fun extractErrorCode(message: String): String {
                val regex = Regex("ERROR_CODE:(\\w+)")
                return regex.find(message)?.groupValues?.get(1) ?: "NONE"
            }
        }

    @Bean
    @StepScope
    fun processedLogEntryJsonWriter(
        @Value("#{jobParameters['date']}") date: String,
    ): FlatFileItemWriter<ProcessedLogEntry> =
        FlatFileItemWriterBuilder<ProcessedLogEntry>()
            .name("processedLogEntryJsonWriter")
            .resource(FileSystemResource("$HOME_DIR/processed_logs/$date/processed_logs.jsonl"))
            .lineAggregator {item ->
                try {
                    objectMapper.writeValueAsString(item)
                } catch (e: JsonProcessingException) {
                    throw RuntimeException(e)
                }
            }
            .build()
}

data class RawLogEntry(
    var dateTime: String = "",
    var level: String = "",
    var message: String = "",
)

data class ProcessedLogEntry(
    private var dateTime: LocalDateTime,
    private var level: LogLevel,
    private val message: String,
    private val errorCode: String,
)

enum class LogLevel {
    INFO, WARN, ERROR, DEBUG, UNKNOWN;

    companion object {
        fun fromString(level: String?): LogLevel {
            if (level == null || level.trim().isEmpty()) return UNKNOWN

            return runCatching { valueOf(level) }
                .getOrElse { UNKNOWN }
        }

    }
}
