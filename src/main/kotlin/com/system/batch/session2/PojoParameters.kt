package com.system.batch.session2

import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
@StepScope
class PojoParameters(
    @param:Value("#{jobParameters[missionName]}")
    val missionName: String,
    @param:Value("#{jobParameters[securityLevel]}")
    val securityLevel: Int,
    @param:Value("#{jobParameters[operationCommander]}")
    val operationCommander: String,
)
