package com.system.batch.session4.redis

import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component

@Component
class AttackCounterItemWriter(
    private val attackCounter: AttackCounter,
) : ItemWriter<AttackLog> {
    override fun write(chunk: Chunk<out AttackLog>) {
        chunk.forEach { attackLog ->
            attackCounter.record(attackLog)
        }
    }
}
