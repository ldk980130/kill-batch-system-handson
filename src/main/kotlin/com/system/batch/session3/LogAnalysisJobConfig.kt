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
import org.springframework.batch.item.file.transform.FieldSet
import org.springframework.batch.item.file.transform.RegexLineTokenizer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.transaction.PlatformTransactionManager


private val logger = KotlinLogging.logger {}

@Configuration
class LogAnalysisJobConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    @Bean
    fun logAnalysisJob(logAnalysisStep: Step): Job {
        return JobBuilder("logAnalysisJob", jobRepository)
            .start(logAnalysisStep)
            .build()
    }

    @Bean
    fun logAnalysisStep(
        logItemReader: FlatFileItemReader<LogEntry>,
        logItemWriter: ItemWriter<LogEntry>,
    ): Step {
        return StepBuilder("logAnalysisStep", jobRepository)
            .chunk<LogEntry, LogEntry>(10, transactionManager)
            .reader(logItemReader)
            .writer(logItemWriter)
            .build()
    }

    @Bean
    @StepScope
    fun logItemReader(
        @Value("#{jobParameters['inputFile']}") inputFile: String,
    ): FlatFileItemReader<LogEntry> =
        FlatFileItemReaderBuilder<LogEntry>()
            .name("logItemReader")
            .resource(ClassPathResource(inputFile))
            .lineTokenizer(
                RegexLineTokenizer().apply {
                    setRegex("\\[\\w+\\]\\[Thread-(\\d+)\\]\\[CPU: \\d+%\\] (.+)")
                }
            )
            .fieldSetMapper { fieldSet: FieldSet ->
                LogEntry(
                    fieldSet.readString(0),
                    fieldSet.readString(1)
                )
            }
            .build()


    @Bean
    fun logItemWriter(): ItemWriter<LogEntry> =
        ItemWriter { items: Chunk<out LogEntry> ->
            items.forEach { item: LogEntry ->
                logger.info { "THD-${item.threadNum}: ${item.message}" }
            }
        }

}

data class LogEntry(
    var threadNum: String = "",
    var message: String = "",
)
