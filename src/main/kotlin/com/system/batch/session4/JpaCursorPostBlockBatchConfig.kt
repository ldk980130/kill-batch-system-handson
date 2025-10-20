package com.system.batch.session4

import com.system.batch.session4.entity.Post
import com.system.batch.session4.entity.Report
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.database.JpaCursorItemReader
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import java.time.LocalDateTime


@Configuration
class JpaCursorPostBlockBatchConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val entityManagerFactory: EntityManagerFactory,
) {
    private val log: KLogger = KotlinLogging.logger {}

    @Bean
    fun postBlockBatchJob(postBlockStep: Step): Job {
        return JobBuilder("postBlockBatchJob", jobRepository)
            .start(postBlockStep)
            .build()
    }

    @Bean
    fun postBlockStep(
        postBlockReader: JpaCursorItemReader<Post>,
        postBlockProcessor: PostBlockProcessor,
        postBlockWriter: ItemWriter<BlockedPost?>,
    ): Step? {
        return StepBuilder("postBlockStep", jobRepository)
            .chunk<Post, BlockedPost>(5, transactionManager)
            .reader(postBlockReader)
            .processor(postBlockProcessor)
            .writer(postBlockWriter)
            .build()
    }

    @Bean
    @StepScope
    fun postBlockReader(
        @Value("#{jobParameters['startDateTime']}") startDateTime: LocalDateTime,
        @Value("#{jobParameters['endDateTime']}") endDateTime: LocalDateTime,
    ): JpaCursorItemReader<Post> =
        JpaCursorItemReaderBuilder<Post>()
            .name("postBlockReader")
            .entityManagerFactory(entityManagerFactory)
            .queryString(
                """
                SELECT p FROM Post p JOIN FETCH p.reports r
                WHERE r.reportedAt >= :startDateTime AND r.reportedAt < :endDateTime
            """.trimIndent()
            )
            .parameterValues(
                mapOf(
                    "startDateTime" to startDateTime,
                    "endDateTime" to endDateTime
                )
            )
            .build()

    @Bean
    fun postBlockWriter(): ItemWriter<BlockedPost> {
        return ItemWriter { items: Chunk<out BlockedPost> ->
            items.forEach {
                log.info { "💀 TERMINATED: $it" }
            }
        }
    }
}

data class BlockedPost(
    val postId: Long,
    val writer: String,
    val title: String,
    val reportCount: Int = 0,
    val blockScore: Double = 0.0,
    val blockedAt: LocalDateTime,
)

@Component
class PostBlockProcessor : ItemProcessor<Post, BlockedPost> {
    override fun process(post: Post): BlockedPost? {
        val blockScore = calculateBlockScore(post.reports)

        // 차단 점수가 기준치를 넘으면 신고로 간주
        if (blockScore >= 7.0) {
            return BlockedPost(
                postId = post.id!!,
                writer = post.writer,
                title = post.title,
                reportCount = post.reports.size,
                blockScore = blockScore,
                blockedAt = LocalDateTime.now(),
            )
        }

        return null // 무죄
    }

    private fun calculateBlockScore(reports: MutableList<Report>): Double {
        return Math.random() * 10 // 0~10 사이의 랜덤 값
    }
}
