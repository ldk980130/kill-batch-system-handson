package com.system.batch.session4.entity

import jakarta.persistence.Entity
import jakarta.persistence.FetchType
import jakarta.persistence.Id
import jakarta.persistence.JoinColumn
import jakarta.persistence.ManyToOne
import jakarta.persistence.Table
import java.time.LocalDateTime


@Entity
@Table(name = "reports")
class Report(
    @Id
    var id: Long? = null,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "post_id")
    private val post: Post,

    var reportType: String, // SPAM, ABUSE, ILLEGAL, FAKE_NEWS ...
    var reporterLevel: Int = 0, // 신고자 신뢰도 (1~5)
    var evidenceData: String, // 증거 데이터 (URL 등)
    var reportedAt: LocalDateTime,
)
