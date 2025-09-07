package com.system.batch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import java.util.concurrent.atomic.AtomicInteger

@Configuration
class SystemTerminationConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    private val log: KLogger = KotlinLogging.logger {}

    private val processesKilled = AtomicInteger(0)

    companion object {
        private const val TERMINATION_TARGET = 5
    }

    @Bean
    fun systemTerminationSimulationJob(): Job =
        JobBuilder("systemTerminationSimulationJob", jobRepository)
            .listener(BigBrotherJobExecutionListener())
            .start(enterWorldStep())
            .next(meetNPCStep())
            .next(defeatProcessStep())
            .next(completeQuestStep())
            .build()

    @Bean
    fun enterWorldStep(): Step =
        StepBuilder("enterWorldStep", jobRepository)
            .listener(BigBrotherStepExecutionListener())
            .tasklet({ contribution, chunkContext ->
                log.info { "System Termination Process is running..." }
                RepeatStatus.FINISHED
            }, transactionManager)
            .build()

    @Bean
    fun meetNPCStep(): Step =
        StepBuilder("meetNPCStep", jobRepository)
            .tasklet({ contribution, chunkContext ->
                log.info { "Meet NPC Process is running..." }
                RepeatStatus.FINISHED
            }, transactionManager)
            .build()

    @Bean
    fun defeatProcessStep(): Step =
        StepBuilder("defeatProcessStep", jobRepository)
            .tasklet({ contribution, chunkContext ->
                val termiated = processesKilled.incrementAndGet()
                log.info { "Defeat Process is running...(현재 $termiated / $TERMINATION_TARGET)" }
                if (termiated < TERMINATION_TARGET) {
                    RepeatStatus.CONTINUABLE
                } else {
                    RepeatStatus.FINISHED
                }
            }, transactionManager)
            .build()

    @Bean
    fun completeQuestStep(): Step =
        StepBuilder("completeQuestStep", jobRepository)
            .tasklet({ contribution, chunkContext ->
                log.info { "Complete Quest Process, $TERMINATION_TARGET Process is completed." }
                RepeatStatus.FINISHED
            }, transactionManager)
            .build()
}
