package com.system.batch.session4.redis

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@Component
class AttackCounter : JobExecutionListener {
    private val logger = KotlinLogging.logger {}

    // 💀공격 타입별 카운트
    private val attackTypeCount = ConcurrentHashMap<AttackType, Int>()

    // 💀IP별 공격 횟수
    private val ipAttackCount = ConcurrentHashMap<String, Int>()

    // 💀시간대별 그룹핑 (시간 부분만 추출)
    private val timeSlotCount = ConcurrentHashMap<Int, Int>()

    // 💀전체 카운트 기록
    private val totalAttacks = AtomicInteger(0)

    fun record(attackLog: AttackLog) {
        val type = attackLog.attackType
        attackTypeCount.merge(type, 1, Int::plus)
        ipAttackCount.merge(attackLog.targetIp ?: UNKNOWN, 1, Int::plus)
        attackLog.timestamp?.hour?.let { hour ->
            timeSlotCount.merge(hour, 1, Int::plus)
        }
        totalAttacks.incrementAndGet()
    }

    fun generateAnalysis(): AttackAnalysisResult {
        val total = getTotalAttacks()
        val attackTypePercentage =
            getAttackTypeCount()
                .mapValues { (_, count) ->
                    "%.1f%%".format(count * 100.0 / total)
                }

        return AttackAnalysisResult(
            totalAttacks = total,
            attackTypeCount = getAttackTypeCount(),
            attackTypePercentage = attackTypePercentage,
            ipAttackCount = getIpAttackCount(),
            timeSlotCount = getTimeSlotCount(),
            mostDangerousIp = findMostDangerousIp(),
            peakHour = findPeakHour(),
            threatLevel = calculateThreatLevel(),
        )
    }

    override fun afterJob(jobExecution: JobExecution) {
        logger.info { "💀 [KILL-9] 공격 분석 작전 성공! 다음 작전을 위해 데이터 정리 중..." }
        reset()
        logger.info { "💀 [KILL-9] 시스템 초기화 완료. 다음 침입자를 기다린다..." }
    }

    private fun reset() {
        attackTypeCount.clear()
        ipAttackCount.clear()
        timeSlotCount.clear()
        totalAttacks.set(0)
    }

    private fun getAttackTypeCount(): Map<AttackType, Int> = attackTypeCount.toMap()

    private fun getIpAttackCount(): Map<String, Int> = ipAttackCount.toMap()

    private fun getTimeSlotCount(): Map<String, Int> =
        timeSlotCount.entries.associate { (hour, count) ->
            "${hour}시" to count
        }

    fun getTotalAttacks(): Int = totalAttacks.get()

    private fun findMostDangerousIp(): String = ipAttackCount.maxByOrNull { it.value }?.key ?: UNKNOWN

    private fun findPeakHour(): String = timeSlotCount.maxByOrNull { it.value }?.key?.let { "${it}시" } ?: UNKNOWN

    private fun calculateThreatLevel(): String {
        val total = totalAttacks.get()
        return when {
            total >= 10 -> "CRITICAL"
            total >= 5 -> "HIGH"
            total >= 2 -> "MEDIUM"
            else -> "LOW"
        }
    }

    companion object {
        private const val UNKNOWN = "Unknown"
    }
}
