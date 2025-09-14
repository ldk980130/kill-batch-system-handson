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
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import org.springframework.transaction.PlatformTransactionManager


private val logger = KotlinLogging.logger {}

@Configuration
class SimpleCsvFileReadJobExConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    @Bean
    fun systemFailureJob(systemFailureStep: Step): Job {
        return JobBuilder("systemFailureJob", jobRepository)
            .start(systemFailureStep)
            .build()
    }

    @Bean
    fun systemFailureStep(
        systemFailureItemReader: FlatFileItemReader<SystemFailure>,
        systemFailureStdoutItemWriter: SystemFailureStdoutItemWriter,
    ): Step {
        return StepBuilder("systemFailureStep", jobRepository)
            .chunk<SystemFailure, SystemFailure>(10, transactionManager)
            .reader(systemFailureItemReader)
            .writer(systemFailureStdoutItemWriter)
            .build()
    }

    @Bean
    @StepScope
    fun systemFailureItemReader(
        @Value("#{jobParameters['inputFile']}") inputFile: String,
    ): FlatFileItemReader<SystemFailure> {
        return FlatFileItemReaderBuilder<SystemFailure>()
            .name("systemFailureItemReader")
            .resource(FileSystemResource(inputFile))
            .delimited()
            .delimiter(",")
            .names(
                "errorId",
                "errorDateTime",
                "severity",
                "processId",
                "errorMessage"
            )
            .targetType(SystemFailure::class.java)
            .linesToSkip(1)
            .build()
    }

    @Bean
    fun systemFailureStdoutItemWriter(): SystemFailureStdoutItemWriter {
        return SystemFailureStdoutItemWriter()
    }

    class SystemFailureStdoutItemWriter : ItemWriter<SystemFailure> {
        @Throws(Exception::class)
        override fun write(chunk: Chunk<out SystemFailure>) {
            for (failure in chunk) {
                logger.info { "Processing system failure: $failure" }
            }
        }
    }
}

data class SystemFailure(
    val errorId: String,
    val errorDateTime: String,
    val severity: String,
    val processId: Int,
    val errorMessage: String,
)

