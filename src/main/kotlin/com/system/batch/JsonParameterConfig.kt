package com.system.batch

import org.springframework.batch.core.converter.JobParametersConverter
import org.springframework.batch.core.converter.JsonJobParametersConverter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class JsonParameterConfig {
    @Bean
    fun jobParametersConverter(): JobParametersConverter = JsonJobParametersConverter()
}
