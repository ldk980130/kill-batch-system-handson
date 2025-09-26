package com.system.batch.session3

import com.system.batch.session3.SimpleCsvFileReadJobExConfig.SystemFailureStdoutItemWriter
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.MultiResourceItemReader
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class MultiResourceItemReaderConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    /**
     *  ./gradlew bootRun --args='--spring.batch.job.name=deathNoteWriteJob outputDir=/Users/does/IdeaProjects/kill-batch-system-handson'
     */
    @Bean
    fun systemFailureMultiStep(
        multiSystemFailureItemReader: MultiResourceItemReader<SystemFailure>,
        systemFailureStdoutItemWriter: SystemFailureStdoutItemWriter,
    ): Step =
        StepBuilder("systemFailureMultiStep", jobRepository)
            .chunk<SystemFailure, SystemFailure>(10, transactionManager)
            .reader(multiSystemFailureItemReader)
            .writer(systemFailureStdoutItemWriter)
            .build()

    @Bean
    @StepScope
    fun multiSystemFailureItemReader(
        @Value("#{jobParameters['inputFilePath']}") inputFilePath: String,
    ): MultiResourceItemReader<SystemFailure> =
        MultiResourceItemReaderBuilder<SystemFailure>()
            .name("multiSystemFailureItemReader")
            .resources(
                ClassPathResource("$inputFilePath/critical-failures.csv"),
                ClassPathResource("$inputFilePath/normal-failures.csv"),
            ).delegate(systemFailureReader())
            .comparator { r1, r2 -> r2.filename!!.compareTo(r1.filename!!) }
            .build()

    @Bean
    fun systemFailureReader(): FlatFileItemReader<SystemFailure> =
        FlatFileItemReaderBuilder<SystemFailure>()
            .name("systemFailureReader")
            .delimited()
            .delimiter(",")
            .names("errorId", "errorDateTime", "severity", "processId", "errorMessage")
            .targetType(SystemFailure::class.java)
            .linesToSkip(1)
            .build()
}
