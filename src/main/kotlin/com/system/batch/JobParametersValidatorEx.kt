package com.system.batch

import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersInvalidException
import org.springframework.batch.core.JobParametersValidator
import org.springframework.stereotype.Component

@Component
class JobParametersValidatorEx : JobParametersValidator {
    override fun validate(parameters: JobParameters?) {
        requireNotNull(parameters) { "파라미터가 NULL입니다" }

        val destructionPower: Long =
            parameters.getLong("destructionPower")
                ?: throw JobParametersInvalidException("destructionPower 파라미터는 필수값입니다")

        if (destructionPower > 9) {
            throw JobParametersInvalidException("파괴력 수준이 허용치를 초과했습니다: $destructionPower(최대 허용치: 9)")
        }
    }
}
