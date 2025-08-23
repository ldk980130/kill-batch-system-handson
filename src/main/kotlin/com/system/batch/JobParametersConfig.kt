package com.system.batch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
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
class JobParametersConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    private val log: KLogger = KotlinLogging.logger {}

    @Bean
    fun processTerminatorJob(terminationStep: Step): Job =
        JobBuilder("processTerminatorJob", jobRepository)
            .start(terminationStep)
            .build()

    @Bean
    fun terminationStep(terminatorTasklet: Tasklet): Step =
        StepBuilder("terminationStep", jobRepository)
            .tasklet(terminatorTasklet, transactionManager)
            .build()

    @Bean
    @StepScope
    fun terminatorTasklet(
        @Value("#{jobParameters['terminatorId']}") terminatorId: String,
        @Value("#{jobParameters['targetCount']}") targetCount: Int,
    ): Tasklet =
        Tasklet { _: StepContribution?, _: ChunkContext? ->
            log.info { "시스템 종결자 정보:" }
            log.info { "ID: $terminatorId" }
            log.info { "제거 대상 수: $targetCount" }
            log.info { "⚡ SYSTEM TERMINATOR $terminatorId 작전을 개시합니다." }
            log.info { "☠️ ${targetCount}개의 프로세스를 종료합니다." }

            for (i in 1..targetCount) {
                log.info { "💀 프로세스 $i 종료 완료!" }
            }

            log.info { "🎯 임무 완료: 모든 대상 프로세스가 종료되었습니다." }
            RepeatStatus.FINISHED
        }
}
