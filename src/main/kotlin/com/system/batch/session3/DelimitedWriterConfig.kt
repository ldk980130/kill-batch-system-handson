package com.system.batch.session3

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.file.FlatFileFooterCallback
import org.springframework.batch.item.file.FlatFileHeaderCallback
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder
import org.springframework.batch.item.file.transform.FieldExtractor
import org.springframework.batch.item.file.transform.RecordFieldExtractor
import org.springframework.batch.item.support.ListItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import org.springframework.transaction.PlatformTransactionManager
import java.io.Writer

@Configuration
class DelimitedWriterConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    @Bean
    fun deathNoteWriteJob(deathNoteWriteStep: Step): Job =
        JobBuilder("deathNoteWriteJob", jobRepository)
            .start(deathNoteWriteStep)
            .build()

    @Bean
    fun deathNoteWriteStep(
        deathNoteListReader: ListItemReader<DeathNote>,
        deathNoteFormatterLineWriter: FlatFileItemWriter<DeathNote>,
    ): Step =
        StepBuilder("deathNoteWriteStep", jobRepository)
            .chunk<DeathNote, DeathNote>(10, transactionManager)
            .reader(deathNoteListReader)
            .writer(deathNoteFormatterLineWriter)
            .build()

    @Bean
    fun deathNoteListReader(): ListItemReader<DeathNote> =
        listOf(
            DeathNote(
                "KILL-001",
                "김배치",
                "2024-01-25",
                "CPU 과부하",
            ),
            DeathNote(
                "KILL-002",
                "사불링",
                "2024-01-26",
                "JVM 스택오버플로우",
            ),
            DeathNote(
                "KILL-003",
                "박탐묘",
                "2024-01-27",
                "힙 메모리 고갈",
            ),
        ).run { ListItemReader(this) }

    @Bean
    @StepScope
    fun deathNoteDelimiterWriter(
        @Value("#{jobParameters['outputDir']}") outputDir: String,
    ): FlatFileItemWriter<DeathNote> =
        FlatFileItemWriterBuilder<DeathNote>()
            .name("deathNoteWriter")
            .resource(FileSystemResource("$outputDir/death_notes.csv"))
            .delimited()
            .delimiter(",")
            .sourceType(DeathNote::class.java)
            .names("victimId", "victimName", "executionDate", "causeOfDeath")
            .headerCallback { writer: Writer -> writer.write("처형ID,피해자명,처형일자,사인") }
            .build()

    @Bean
    @StepScope
    fun deathNoteCustomExtractorWriter(
        @Value("#{jobParameters['outputDir']}") outputDir: String,
    ): FlatFileItemWriter<DeathNote> =
        FlatFileItemWriterBuilder<DeathNote>()
            .name("deathNoteWriter")
            .resource(FileSystemResource("$outputDir/death_notes.csv"))
            .delimited()
            .delimiter(",")
            .fieldExtractor(fieldExtractor())
            .headerCallback { writer: Writer -> writer.write("처형ID,피해자명,처형일자,사인") }
            .build()

    fun fieldExtractor(): FieldExtractor<DeathNote> =
        RecordFieldExtractor(DeathNote::class.java)
            .apply { setNames("victimId", "executionDate", "causeOfDeath") }

    @Bean
    @StepScope
    fun deathNoteFormatterLineWriter(
        @Value("#{jobParameters['outputDir']}") outputDir: String?,
    ): FlatFileItemWriter<DeathNote> =
        FlatFileItemWriterBuilder<DeathNote>()
            .name("deathNoteWriter")
            .resource(FileSystemResource("$outputDir/death_note_report.txt"))
            .formatted()
            .format("처형 ID: %s | 처형일자: %s | 피해자: %s | 사인: %s")
            .sourceType(DeathNote::class.java)
            .names("victimId", "executionDate", "victimName", "causeOfDeath")
            .headerCallback { writer: Writer -> writer!!.write("================= 처형 기록부 =================") }
            .footerCallback { writer: Writer -> writer!!.write("================= 처형 완료 ==================") }
            .build()
}

data class DeathNote(
    val victimId: String,
    val victimName: String,
    val executionDate: String,
    val causeOfDeath: String,
)
