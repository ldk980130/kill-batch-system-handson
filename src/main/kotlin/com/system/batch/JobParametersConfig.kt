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
    fun terminationStep(terminatorTaskletEnum: Tasklet): Step =
        StepBuilder("terminationStep", jobRepository)
            .tasklet(terminatorTaskletEnum, transactionManager)
            .build()

    @Bean
    @StepScope
    fun terminatorTasklet(
        @Value("#{jobParameters['terminatorId']}") terminatorId: String,
        @Value("#{jobParameters['targetCount']}") targetCount: Int,
    ): Tasklet =
        Tasklet { _: StepContribution, _: ChunkContext ->
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

    @Bean
    @StepScope
    fun terminatorTaskletEnum(
        @Value("#{jobParameters['questDifficulty']}") questDifficulty: QuestDifficulty,
    ): Tasklet =
        Tasklet { _: StepContribution, _: ChunkContext ->
            log.info { "⚔️ 시스템 침투 작전 개시!" }
            log.info { "임무 난이도: $questDifficulty" }
            // 난이도에 따른 보상 계산
            val baseReward = 100
            val rewardMultiplier =
                when (questDifficulty) {
                    QuestDifficulty.EASY -> 1
                    QuestDifficulty.NORMAL -> 2
                    QuestDifficulty.HARD -> 3
                    QuestDifficulty.EXTREME -> 5
                }
            val totalReward = baseReward * rewardMultiplier
            log.info { "💥 시스템 해킹 진행 중..." }
            log.info { "🏆 시스템 장악 완료!" }
            log.info { "💰 획득한 시스템 리소스: $totalReward 메가바이트" }
            RepeatStatus.FINISHED
        }

    enum class QuestDifficulty { EASY, NORMAL, HARD, EXTREME }
}
