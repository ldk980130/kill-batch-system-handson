package com.system.batch.session4.entity

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.NamedQuery
import jakarta.persistence.OneToMany
import jakarta.persistence.Table

@Entity
@Table(name = "posts")
@NamedQuery(
    name = "Post.findByReportsReportedAtBetween",
    query = "SELECT p FROM Post p JOIN FETCH p.reports r WHERE r.reportedAt >= :startDateTime AND r.reportedAt < :endDateTime",
)
class Post(
    @Id
    var id: Long? = null,
    var title: String,
    var content: String,
    var writer: String,
) {
    @OneToMany(mappedBy = "post")
    val reports: MutableList<Report> = mutableListOf()
        get() = field.toMutableList()
}
