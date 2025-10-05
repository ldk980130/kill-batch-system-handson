package com.system.batch.session3

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.file.*
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder
import org.springframework.batch.item.file.builder.MultiResourceItemWriterBuilder
import org.springframework.batch.item.support.ListItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import org.springframework.transaction.PlatformTransactionManager
import java.io.Writer
import java.time.LocalDate
import java.time.format.DateTimeFormatter


@Configuration
class MultiResourceItemWriterConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    @Bean
    fun deathNoteMultiResourceWriteJob(deathNoteMultiResourceWriteStep: Step): Job =
        JobBuilder("deathNoteMultiResourceWriteJob", jobRepository)
            .start(deathNoteMultiResourceWriteStep)
            .build()

    @Bean
    fun deathNoteMultiResourceWriteStep(
        deathNoteMultiListReader: ListItemReader<DeathNote>,
        multiResourceItemWriter: MultiResourceItemWriter<DeathNote?>,
    ): Step =
        StepBuilder("deathNoteMultiResourceWriteStep", jobRepository)
            .chunk<DeathNote, DeathNote>(10, transactionManager)
            .reader(deathNoteMultiListReader)
            .writer(multiResourceItemWriter)
            .build()

    @Bean
    fun deathNoteMultiListReader(): ListItemReader<DeathNote> {
        val deathNotes: MutableList<DeathNote> = ArrayList()
        repeat(15) {
            val id = String.format("KILL-%03d", it)
            val date = LocalDate.now().plusDays(it.toLong())
            deathNotes.add(
                DeathNote(
                    id,
                    "피해자$it",
                    date.format(DateTimeFormatter.ISO_DATE),
                    "처형사유$it"
                )
            )
        }
        return ListItemReader<DeathNote>(deathNotes)
    }

    @Bean
    @StepScope
    fun multiResourceItemWriter(
        @Value("#{jobParameters['outputDir']}") outputDir: String?,
    ): MultiResourceItemWriter<DeathNote?> {
        return MultiResourceItemWriterBuilder<DeathNote?>()
            .name("multiDeathNoteWriter")
            .resource(FileSystemResource("$outputDir/death_note"))
            .itemCountLimitPerResource(10)
            .delegate(delegateItemWriter())
            .resourceSuffixCreator { index: Int -> String.format("_%03d.txt", index) }
            .build()
    }

    @Bean
    fun delegateItemWriter(): FlatFileItemWriter<DeathNote> {
        return FlatFileItemWriterBuilder<DeathNote>()
            .name("deathNoteWriter")
            .formatted()
            .format("처형 ID: %s | 처형일자: %s | 피해자: %s | 사인: %s")
            .sourceType(DeathNote::class.java)
            .names("victimId", "executionDate", "victimName", "causeOfDeath")
            .headerCallback { writer: Writer -> writer.write("================= 처형 기록부 =================") }
            .footerCallback { writer: Writer -> writer.write("================= 처형 완료 ==================") }
            .build()
    }
}
