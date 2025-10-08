package com.system.batch.session3

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller
import org.springframework.batch.item.json.JacksonJsonObjectReader
import org.springframework.batch.item.json.JsonFileItemWriter
import org.springframework.batch.item.json.JsonItemReader
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.FileSystemResource
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class JsonJobConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    @Bean
    fun systemDeathJob(systemDeathStep: Step): Job =
        JobBuilder("systemDeathJob", jobRepository)
            .start(systemDeathStep)
            .build()

    @Bean
    fun systemDeathStep(
        systemDeathReader: JsonItemReader<SystemDeath>,
        systemDeathJsonWriter: JsonFileItemWriter<SystemDeath>,
    ): Step =
        StepBuilder("systemDeathStep", jobRepository)
            .chunk<SystemDeath, SystemDeath>(10, transactionManager)
            .reader(systemDeathReader)
            .writer(systemDeathJsonWriter)
            .build()

    @Bean
    @StepScope
    fun systemDeathReader(
        @Value("#{jobParameters['inputFile']}") inputFile: String,
        objectMapper: ObjectMapper,
    ): JsonItemReader<SystemDeath> =
        JsonItemReaderBuilder<SystemDeath>()
            .name("systemDeathReader")
            .jsonObjectReader(JacksonJsonObjectReader(objectMapper, SystemDeath::class.java))
            .resource(ClassPathResource(inputFile))
            .build()

//    @Bean
//    @StepScope
//    fun systemDeathReader(
//        @Value("#{jobParameters['inputFile']}") inputFile: String,
//    ): FlatFileItemReader<SystemDeath> {
//        return FlatFileItemReaderBuilder<SystemDeath>()
//            .name("systemDeathReader")
//            .resource(ClassPathResource(inputFile))
//            .lineMapper { line: String, _: Int ->
//                objectMapper.readValue(line, SystemDeath::class.java)
//            }
//            .recordSeparatorPolicy(JsonRecordSeparatorPolicy())
//            .build()
//    }

    @Bean
    @StepScope
    fun systemDeathJsonWriter(
        @Value("#{jobParameters['outputDir']}") outputDir: String,
    ): JsonFileItemWriter<SystemDeath> =
        JsonFileItemWriterBuilder<SystemDeath>()
            .name("logEntryJsonWriter")
            .jsonObjectMarshaller(JacksonJsonObjectMarshaller())
            .resource(FileSystemResource("$outputDir/death_notes.json"))
            .build()
}

data class SystemDeath(
    var command: String,
    var cpu: Int,
    var status: String,
)
