package com.system.batch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import org.springframework.stereotype.Component

private val log: KLogger = KotlinLogging.logger {}

@Component
class BigBrotherJobExecutionListener : JobExecutionListener {
    override fun beforeJob(jobExecution: JobExecution) {
        log.info { "시스템 감시 시작. 모든 작업을 내 통제 하에 둔다." }
    }

    override fun afterJob(jobExecution: JobExecution) {
        log.info { "작업 종료. 할당된 자원 정리 완료." }
        log.info { "시스템 상태: ${jobExecution.status}" }
    }
}

@Component
class BigBrotherStepExecutionListener : StepExecutionListener {
    override fun beforeStep(stepExecution: StepExecution) {
        log.info { "Step 구역 감시 시작. 모든 행동이 기록된다." }
    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus {
        log.info { "Step 감시 종료. 모든 행동이 기록되었다." }
        log.info { "Big Brother의 감시망에서 벗어날 수 없을 것이다." }
        return ExitStatus.COMPLETED
    }
}
