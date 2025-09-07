package com.system.batch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import org.springframework.batch.core.annotation.AfterJob
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeJob
import org.springframework.batch.core.annotation.BeforeStep
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

@Component
class ServerRoomInfiltrationListener {
    @BeforeJob
    fun infiltrateServerRoom(jobExecution: JobExecution) {
        log.info { "판교 서버실 침투 시작. 보안 시스템 무력화 진행중." }
    }

    @AfterJob
    fun escapeServerRoom(jobExecution: JobExecution) {
        log.info { "파괴 완료. 침투 결과: ${jobExecution.status}" }
    }
}

@Component
class ServerRackControlListener {
    @BeforeStep
    fun accessServerRack(stepExecution: StepExecution) {
        log.info { "서버랙 접근 시작. 콘센트를 찾는 중." }
    }

    @AfterStep
    fun leaveServerRack(stepExecution: StepExecution): ExitStatus {
        log.info { "코드를 뽑아버렸다." }
        return ExitStatus("POWER_DOWN")
    }
}
