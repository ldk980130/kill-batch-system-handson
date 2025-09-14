package com.system.batch.session2

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.stereotype.Component

@Component
class ProgrammingAccessJobParameterTasklet : Tasklet {
    private val log: KLogger = KotlinLogging.logger {}

    override fun execute(
        contribution: StepContribution,
        chunkContext: ChunkContext,
    ): RepeatStatus {
        val jobParameters: JobParameters =
            chunkContext.stepContext.stepExecution.jobParameters

        val targetSystem = jobParameters.getString("system.target")
        val destructionLevel: Long = jobParameters.getLong("system.destruction.level")!!

        log.info { targetSystem }
        log.info { destructionLevel }

        return RepeatStatus.FINISHED
    }
}
