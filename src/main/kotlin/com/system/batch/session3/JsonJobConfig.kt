package com.system.batch.session3

import com.fasterxml.jackson.databind.ObjectMapper
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
import org.springframework.batch.item.file.separator.JsonRecordSeparatorPolicy
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.transaction.PlatformTransactionManager


@Configuration
class JsonJobConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val objectMapper: ObjectMapper,
) {
    @Bean
    fun systemDeathJob(systemDeathStep: Step): Job {
        return JobBuilder("systemDeathJob", jobRepository)
            .start(systemDeathStep)
            .build()
    }

    @Bean
    fun systemDeathStep(
        systemDeathReader: FlatFileItemReader<SystemDeath>,
    ): Step {
        return StepBuilder("systemDeathStep", jobRepository)
            .chunk<SystemDeath, SystemDeath>(10, transactionManager)
            .reader(systemDeathReader)
            .writer(ItemWriter { items: Chunk<out SystemDeath> -> items.forEach { x -> println(x) } })
            .build()
    }

    @Bean
    @StepScope
    fun systemDeathReader(
        @Value("#{jobParameters['inputFile']}") inputFile: String,
    ): FlatFileItemReader<SystemDeath> {
        return FlatFileItemReaderBuilder<SystemDeath>()
            .name("systemDeathReader")
            .resource(ClassPathResource(inputFile))
            .lineMapper { line: String, _: Int ->
                objectMapper.readValue(line, SystemDeath::class.java)
            }
            .recordSeparatorPolicy(JsonRecordSeparatorPolicy())
            .build()
    }
}

data class SystemDeath(
    val command: String,
    val cpu: Int,
    val status: String,
)
