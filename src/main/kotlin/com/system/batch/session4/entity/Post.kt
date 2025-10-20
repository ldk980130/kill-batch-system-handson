package com.system.batch.session4.entity

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.OneToMany
import jakarta.persistence.Table


@Entity
@Table(name = "posts")
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
