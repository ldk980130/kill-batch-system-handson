package com.system.batch.session4.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "blocked_posts")
class BlockedPost(
    @Id
    @Column(name = "post_id")
    val postId: Long,

    val writer: String,

    val title: String,

    val reportCount: Int = 0,

    val blockScore: Double = 0.0,

    val blockedAt: LocalDateTime,
)
