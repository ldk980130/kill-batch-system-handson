package com.system.batch.session2

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.listener.ExecutionContextPromotionListener
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class ExecutionContextPromotionListenerExConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    private val logger = KotlinLogging.logger {}

    @Bean
    fun systemTerminationJob(
        scanningStep: Step,
        eliminationStep: Step,
    ): Job =
        JobBuilder("systemTerminationJob", jobRepository)
            .start(scanningStep)
            .next(eliminationStep)
            .build()

    @Bean
    fun scanningStep(): Step =
        StepBuilder("scanningStep", jobRepository)
            .tasklet({ contribution, chunkContext ->
                val target = "판교 서버실"

                val executionContext = contribution.stepExecution.executionContext
                executionContext.put("targetSystem", target)

                RepeatStatus.FINISHED
            }, transactionManager)
            .listener(promotionListener())
            .build()

    @Bean
    fun eliminationStep(eliminationTasklet: Tasklet): Step =
        StepBuilder("eliminationSte", jobRepository)
            .tasklet(eliminationTasklet, transactionManager)
            .build()

    @Bean
    @StepScope
    fun eliminationTasklet(
        @Value("#{jobExecutionContext['targetSystem']}") targetStatus: String,
    ): Tasklet =
        Tasklet { _: StepContribution, _: ChunkContext ->
            logger.info { "시스템 제거 작업 실행: $targetStatus" }
            RepeatStatus.FINISHED
        }

    @Bean
    fun promotionListener(): ExecutionContextPromotionListener =
        ExecutionContextPromotionListener()
            .apply { setKeys(arrayOf("targetSystem")) }
}
