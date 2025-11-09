package com.system.batch.session4

import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDateTime

class RedisReaderWriterConfig

// 공격 로그 데이터 모델
data class AttackLog(
    var id: Long = 0,
    var timestamp: LocalDateTime? = null,
    var targetIp: String? = null,
    var attackType: AttackType = AttackType.UNKNOWN,
    var payload: String? = null,
) {
    fun setAttackType(attackTypeStr: String) {
        this.attackType = AttackType.fromString(attackTypeStr)
    }
}

// 공격 타입 enum
enum class AttackType(
    private val displayName: String,
) {
    SQL_INJECTION("SQL Injection"),
    XSS("Cross-Site Scripting (XSS)"),
    DDOS("DDoS"),
    BRUTE_FORCE("Brute Force"),
    UNKNOWN("Unknown"),
    ;

    @JsonValue
    fun getDisplayName(): String = displayName

    companion object {
        fun fromString(attackType: String?): AttackType {
            if (attackType == null) return UNKNOWN

            return when (attackType.lowercase()) {
                "sql injection" -> SQL_INJECTION
                "cross-site scripting (xss)", "xss" -> XSS
                "ddos" -> DDOS
                "brute force" -> BRUTE_FORCE
                else -> UNKNOWN
            }
        }
    }
}

// 공격 분석 결과 모델
data class AttackAnalysisResult(
    val totalAttacks: Int = 0,
    val attackTypeCount: Map<AttackType, Int> = emptyMap(),
    val attackTypePercentage: Map<AttackType, String> = emptyMap(),
    val ipAttackCount: Map<String, Int> = emptyMap(),
    val timeSlotCount: Map<String, Int> = emptyMap(),
    val mostDangerousIp: String? = null,
    val peakHour: String? = null,
    val threatLevel: String? = null,
) {
    companion object {
        private val PRETTY_MAPPER =
            jacksonObjectMapper().apply {
                enable(SerializationFeature.INDENT_OUTPUT)
                disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                registerModule(JavaTimeModule())
            }
    }

    override fun toString(): String =
        try {
            """
            💀 ========== KILL-9 공격 분석 결과 ==========
            ${PRETTY_MAPPER.writeValueAsString(this)}
            💀 ============================================
            """.trimIndent()
        } catch (e: Exception) {
            super.toString()
        }
}
