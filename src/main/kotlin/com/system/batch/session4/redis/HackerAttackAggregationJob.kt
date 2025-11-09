package com.system.batch.session4.redis

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.redis.RedisItemReader
import org.springframework.batch.item.redis.builder.RedisItemReaderBuilder
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.ScanOptions
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.transaction.PlatformTransactionManager

private val logger = KotlinLogging.logger {}

@Configuration
class HackerAttackAggregationJob(
    private val redisConnectionFactory: RedisConnectionFactory,
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    private val objectMapper =
        jacksonObjectMapper().apply {
            registerModule(JavaTimeModule())
        }

    private val redisTemplate: RedisTemplate<String, AttackLog> by lazy {
        RedisTemplate<String, AttackLog>().apply {
            connectionFactory = redisConnectionFactory
            keySerializer = StringRedisSerializer()
            valueSerializer = Jackson2JsonRedisSerializer(objectMapper, AttackLog::class.java)
            afterPropertiesSet()
        }
    }

    @Bean
    fun aggregateHackerAttackJob(
        aggregateAttackStep: Step,
        reportAttackStep: Step,
        attackCounter: AttackCounter,
    ): Job =
        JobBuilder("aggregateHackerAttackJob", jobRepository)
            .start(aggregateAttackStep)
            .next(reportAttackStep)
            .listener(attackCounter)
            .build()

    @Bean
    fun aggregateAttackStep(
        attackLogReader: RedisItemReader<String, AttackLog>,
        attackCounterItemWriter: AttackCounterItemWriter,
    ): Step =
        StepBuilder("aggregateAttackStep", jobRepository)
            .chunk<AttackLog, AttackLog>(10, transactionManager)
            .reader(attackLogReader)
            .processor { item ->
                logger.info { item }
                item
            }.writer(attackCounterItemWriter)
            .build()

    @Bean
    fun reportAttackStep(attackCounter: AttackCounter): Step =
        StepBuilder("reportAttackStep", jobRepository)
            .tasklet({ _, _ ->
                logger.info { attackCounter.generateAnalysis() }
                RepeatStatus.FINISHED
            }, transactionManager)
            .build()

    @Bean
    fun attackLogReader(): RedisItemReader<String, AttackLog> =
        RedisItemReaderBuilder<String, AttackLog>()
            .redisTemplate(redisTemplate)
            .scanOptions(
                ScanOptions
                    .scanOptions()
                    .match("attack:*") // attack: 으로 시작하는 키만 스캔
                    .count(10)
                    .build(),
            ).build()
}
