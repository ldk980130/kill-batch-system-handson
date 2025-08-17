package com.system.batch

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import kotlin.system.exitProcess

@SpringBootApplication
class KillBatchSystemHandsonApplication

fun main(args: Array<String>) {
    // 배치 애플리케이션은 작업 완료 후 명확히 종료되어야 함
    // exitProcess()로 프로세스를 완전 종료하고, exit code(0:성공, 1+:실패)를 OS/스케줄러에 전달
    // 이를 통해 Jenkins, cron 등 외부 시스템이 배치 상태를 인식하고 자동화된 후처리 가능
    exitProcess(SpringApplication.exit(runApplication<KillBatchSystemHandsonApplication>(*args)))
}
