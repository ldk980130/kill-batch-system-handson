package com.system.batch

import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
class ExecutionContextEx {
    @Bean
    @JobScope
    fun systemDestructionTasklet(
        @Value("#{jobExecutionContext['previousSystemState']}") prevState: String,
    ): Tasklet? {
        // JobExecution의 ExecutionContext에서 이전 시스템 상태를 주입받는다
        TODO()
    }

    @Bean
    @StepScope
    fun infiltrationTasklet(
        @Value("#{stepExecutionContext['targetSystemStatus']}") targetStatus: String,
    ): Tasklet? {
        // StepExecution의 ExecutionContext에서 타겟 시스템 상태를 주입받는다
        TODO()
    }
}
