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
import org.springframework.core.io.ClassPathResource
import org.springframework.transaction.PlatformTransactionManager
import java.beans.PropertyEditor
import java.beans.PropertyEditorSupport
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


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
            .resource(ClassPathResource(inputFile))
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
            .customEditors(mapOf(LocalDateTime::class.java to dateTimeEditor()))
            .build()
    }

//    @Bean
//    @StepScope
//    fun systemFailureItemReader(
//        @Value("#{jobParameters['inputFile']}") inputFile: String,
//    ): FlatFileItemReader<SystemFailure> {
//        return FlatFileItemReaderBuilder<SystemFailure>()
//            .name("systemFailureItemReader")
//            .resource(ClassPathResource(inputFile))
//            .fixedLength()
//            .columns(
//                Range(1, 8),  // errorId: ERR001 + 공백 2칸
//                Range(9, 29),  // errorDateTime: 날짜시간 + 공백 2칸
//                Range(30, 39),  // severity: CRITICAL/FATAL + 패딩
//                Range(40, 45),  // processId: 1234 + 공백 2칸
//                Range(46, 66) // errorMessage: 메시지 + \n
//            )
//            .names("errorId", "errorDateTime", "severity", "processId", "errorMessage")
//            .targetType(SystemFailure::class.java)
//            .build()
//    }

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

    private fun dateTimeEditor(): PropertyEditor {
        return object : PropertyEditorSupport() {
            override fun setAsText(text: String) {
                val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                setValue(LocalDateTime.parse(text, formatter))
            }
        }
    }
}

data class SystemFailure(
    var errorId: String = "",
    var errorDateTime: LocalDateTime? = null,
    var severity: String = "",
    var processId: Int = 0,
    var errorMessage: String = "",
)

