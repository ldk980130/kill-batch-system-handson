package com.system.batch.session3

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.item.file.mapping.FieldSetMapper
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer
import org.springframework.batch.item.file.transform.FieldSet
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.transaction.PlatformTransactionManager


private val logger = KotlinLogging.logger {}

@Configuration
class PatternMatchingCompositeLineMapperConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    @Bean
    fun systemLogJob(systemLogStep: Step): Job {
        return JobBuilder("systemLogJob", jobRepository)
            .start(systemLogStep)
            .build()
    }

    @Bean
    fun systemLogStep(
        systemLogReader: FlatFileItemReader<SystemLog>,
        systemLogWriter: ItemWriter<SystemLog>,
    ): Step {
        return StepBuilder("systemLogStep", jobRepository)
            .chunk<SystemLog, SystemLog>(10, transactionManager)
            .reader(systemLogReader)
            .writer(systemLogWriter)
            .build()
    }

    @Bean
    @StepScope
    fun systemLogReader(
        @Value("#{jobParameters['inputFile']}") inputFile: String,
    ): FlatFileItemReader<SystemLog> {
        return FlatFileItemReaderBuilder<SystemLog>()
            .name("systemLogReader")
            .resource(ClassPathResource(inputFile))
            .lineMapper(systemLogLineMapper())
            .build()
    }

    @Bean
    fun systemLogLineMapper(): PatternMatchingCompositeLineMapper<SystemLog> =
        PatternMatchingCompositeLineMapper<SystemLog>()
            .apply {
                setTokenizers(
                    mapOf(
                        "ERROR*" to errorLineTokenizer(),
                        "ABORT*" to abortLineTokenizer(),
                        "COLLECT*" to collectLineTokenizer(),
                    )
                )
                setFieldSetMappers(
                    mapOf(
                        "ERROR*" to ErrorFieldSetMapper(),
                        "ABORT*" to AbortFieldSetMapper(),
                        "COLLECT*" to CollectFieldSetMapper(),
                    )
                )
            }


    @Bean
    fun errorLineTokenizer(): DelimitedLineTokenizer =
        DelimitedLineTokenizer(",").apply {
            setNames("type", "application", "errorType", "timestamp", "message", "resourceUsage", "logPath")
        }

    @Bean
    fun abortLineTokenizer(): DelimitedLineTokenizer =
        DelimitedLineTokenizer(",").apply {
            setNames("type", "application", "errorType", "timestamp", "message", "exitCode", "processPath", "status")
        }

    @Bean
    fun collectLineTokenizer(): DelimitedLineTokenizer =
        DelimitedLineTokenizer(",").apply {
            setNames("type", "dumpType", "processId", "timestamp", "dumpPath")
        }


    @Bean
    fun systemLogWriter(): ItemWriter<SystemLog> =
        ItemWriter { items: Chunk<out SystemLog> ->
            items.forEach { logger.info { it } }
        }


    class ErrorFieldSetMapper : FieldSetMapper<SystemLog> {
        override fun mapFieldSet(fieldSet: FieldSet): SystemLog =
            SystemLog.Error(
                type = fieldSet.readString("type"),
                timestamp = fieldSet.readString("timestamp"),
                application = fieldSet.readString("application"),
                errorType = fieldSet.readString("errorType"),
                message = fieldSet.readString("message"),
                resourceUsage = fieldSet.readString("resourceUsage"),
                logPath = fieldSet.readString("logPath"),
            )
    }

    class AbortFieldSetMapper : FieldSetMapper<SystemLog> {
        override fun mapFieldSet(fieldSet: FieldSet): SystemLog =
            SystemLog.Abort(
                type = fieldSet.readString("type"),
                timestamp = fieldSet.readString("timestamp"),
                application = fieldSet.readString("application"),
                errorType = fieldSet.readString("errorType"),
                message = fieldSet.readString("message"),
                exitCode = fieldSet.readString("exitCode"),
                processPath = fieldSet.readString("processPath"),
                status = fieldSet.readString("status"),
            )
    }

    class CollectFieldSetMapper : FieldSetMapper<SystemLog> {
        override fun mapFieldSet(fieldSet: FieldSet): SystemLog =
            SystemLog.Collect(
                type = fieldSet.readString("type"),
                timestamp = fieldSet.readString("timestamp"),
                dumpType = fieldSet.readString("dumpType"),
                processId = fieldSet.readString("processId"),
                dumpPath = fieldSet.readString("dumpPath"),
            )
    }
}

sealed interface SystemLog {
    val type: String
    val timestamp: String

    data class Error(
        override val type: String,
        override val timestamp: String,
        val application: String,
        val errorType: String,
        val message: String,
        val resourceUsage: String,
        val logPath: String,
    ) : SystemLog

    data class Abort(
        override val type: String,
        override val timestamp: String,
        val application: String,
        val errorType: String,
        val message: String,
        val exitCode: String,
        val processPath: String,
        val status: String,
    ) : SystemLog

    data class Collect(
        override val type: String,
        override val timestamp: String,
        val dumpType: String,
        val processId: String,
        val dumpPath: String,
    ) : SystemLog
}
