package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.service.CleanupUntrackedTableFoldersService
import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.inject.Inject
import org.jboss.logging.Logger

@QuarkusMain
class App : QuarkusApplication {
    private val logger = Logger.getLogger(App::class.java)

    @Inject
    lateinit var cleanupService: CleanupUntrackedTableFoldersService

    override fun run(vararg args: String): Int {
        logger.info("Cleanup untracked table folders job started")

        cleanupService.run()

        logger.info("Cleanup untracked table folders job finished")
        return 0
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            Quarkus.run(App::class.java, *args)
        }
    }
}
