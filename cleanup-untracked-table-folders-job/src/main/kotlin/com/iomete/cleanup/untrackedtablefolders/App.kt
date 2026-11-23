package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import com.iomete.cleanup.untrackedtablefolders.service.CleanupUntrackedTableFoldersService
import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.inject.Inject

@QuarkusMain
class App : QuarkusApplication {
    private val logger = SafeLogger.getLogger(App::class.java)

    @Inject
    lateinit var cleanupService: CleanupUntrackedTableFoldersService

    // A failure is logged here, sanitized, and exits with 1. Quarkus would log an exception with its raw messages and
    // would not catch an Error at all.
    override fun run(vararg args: String): Int {
        logger.info("Cleanup untracked table folders job started")

        return try {
            cleanupService.run()
            logger.info("Cleanup untracked table folders job finished")
            0
        } catch (error: Throwable) {
            logger.error("Cleanup untracked table folders job failed", error)
            1
        }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            Quarkus.run(App::class.java, *args)
        }
    }
}
