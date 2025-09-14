package com.system.batch.session2

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import kotlin.random.Random

private val logger = KotlinLogging.logger {}

@Configuration
class DynamicParameterExecutionContextExConfig(
    val infiltrationPlanListener: InfiltrationPlanListener,
) {
    @Bean
    fun systemInfiltrationJob(
        jobRepository: JobRepository,
        reconStep: Step,
        attackStep: Step,
    ): Job =
        JobBuilder("systemInfiltrationJob", jobRepository)
            .listener(infiltrationPlanListener)
            .start(reconStep)
            .next(attackStep)
            .build()

    @Bean
    fun reconStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
    ): Step =
        StepBuilder("reconStep", jobRepository)
            .tasklet(
                { _: StepContribution, chunkContext: ChunkContext ->
                    val infiltrationPlan =
                        chunkContext
                            .stepContext
                            .jobExecutionContext["infiltrationPlan"] as Map<String, String>
                    logger.info { "침투 준비 단계: ${infiltrationPlan["targetSystem"]}" }
                    logger.info { "필요한 도구: ${infiltrationPlan["requiredTools"]}" }
                    RepeatStatus.FINISHED
                },
                transactionManager,
            ).build()

    @Bean
    fun attackStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        attackStepTasklet: Tasklet,
    ): Step =
        StepBuilder("attackStep", jobRepository)
            .tasklet(attackStepTasklet, transactionManager)
            .build()

    @Bean
    @StepScope
    fun attackStepTasklet(
        @Value("#{jobExecutionContext['infiltrationPlan']}") infiltrationPlan: Map<String, Any>,
    ): Tasklet =
        Tasklet { contribution: StepContribution, _: ChunkContext ->
            logger.info { "시스템 공격 중: ${infiltrationPlan["targetSystem"]}" }
            logger.info { "목표: ${infiltrationPlan["objective"]}" }

            val infiltrationSuccess = Random.nextBoolean()
            val infiltrationResult = if (infiltrationSuccess) "TERMINATED" else "DETECTED"

            val executionContext: ExecutionContext = contribution.stepExecution.jobExecution.executionContext
            executionContext.put("infiltrationResult", infiltrationResult)

            RepeatStatus.FINISHED
        }
}

@Component
class InfiltrationPlanListener : JobExecutionListener {
    override fun beforeJob(jobExecution: JobExecution) {
        val executionContext: ExecutionContext = jobExecution.executionContext

        val infiltrationPlan: Map<String, String> = generateInfiltrationPlan()
        executionContext.put("infiltrationPlan", infiltrationPlan)

        logger.info { "새로운 침투 계획이 준비됐다: ${infiltrationPlan["targetSystem"]}" }
    }

    private fun generateInfiltrationPlan(): Map<String, String> =
        mapOf(
            "targetSystem" to listOf("판교 서버실", "안산 데이터센터").random(),
            "objective" to listOf("kill -9 실행", "rm -rf 전개", "chmod 000 적용", "/dev/null로 리다이렉션").random(),
            "targetData" to listOf("코어 덤프 파일", "시스템 로그", "설정 파일", "백업 데이터").random(),
            "requiredTools" to listOf("USB 킬러", "널 바이트 인젝터", "커널 패닉 유발기", "메모리 시퍼너").random(),
        )

    override fun afterJob(jobExecution: JobExecution) {
        val executionContext: ExecutionContext = jobExecution.executionContext

        val infiltrationResult = executionContext["infiltrationResult"]

        @Suppress("UNCHECKED_CAST")
        val infiltrationPlan = executionContext["infiltrationPlan"] as Map<String, String>

        logger.info { "타겟 '${infiltrationPlan["targetSystem"]}' 침투 결과: $infiltrationResult" }

        when (infiltrationResult) {
            "TERMINATED" -> logger.info { "시스템 제거 완료. 다음 타겟 검색 중..." }
            else -> logger.info { "철수한다. 다음 기회를 노리자." }
        }
    }
}
