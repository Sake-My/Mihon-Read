package eu.kanade.tachiyomi.ui.reader.loader

import android.content.ContentResolver
import android.content.Context
import android.graphics.Bitmap
import android.graphics.Color
import android.graphics.Matrix
import android.graphics.pdf.PdfRenderer
import android.os.ParcelFileDescriptor
import com.hippo.unifile.UniFile
import eu.kanade.tachiyomi.source.model.Page
import eu.kanade.tachiyomi.ui.reader.model.ReaderPage
import java.io.File
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/**
 * Loader used to render a local PDF file into reader pages.
 */
internal class PdfPageLoader(
    private val context: Context,
    private val file: UniFile,
) : PageLoader() {

    private val fileDescriptor = openFileDescriptor(context, file)
    private val renderer = PdfRenderer(fileDescriptor)
    private val renderMutex = Mutex()
    private val cacheDir = File(
        context.cacheDir,
        "reader-pdf/${file.uri.hashCode()}-${System.nanoTime()}",
    )
    private val targetWidth = min(
        context.resources.displayMetrics.widthPixels,
        context.resources.displayMetrics.heightPixels,
    ).coerceAtLeast(1)

    override var isLocal: Boolean = true

    override suspend fun getPages(): List<ReaderPage> {
        return List(renderer.pageCount) { index ->
            ReaderPage(index, url = "${file.uri}#page=$index")
        }
    }

    override suspend fun loadPage(page: ReaderPage) {
        renderPage(page)

        val pages = page.chapter.pages ?: return
        pages.getOrNull(page.index - 1)?.let(::preloadPage)
        pages.getOrNull(page.index + 1)?.let(::preloadPage)
    }

    override fun recycle() {
        super.recycle()
        renderer.close()
        fileDescriptor.close()
        cacheDir.deleteRecursively()
    }

    private suspend fun preloadPage(page: ReaderPage) {
        if (page.status == Page.State.Ready) return

        try {
            renderPage(page, isPreload = true)
        } catch (_: Throwable) {
        }
    }

    private suspend fun renderPage(
        page: ReaderPage,
        isPreload: Boolean = false,
    ) {
        if (page.status == Page.State.Ready) return

        try {
            val renderedFile = renderMutex.withLock {
                check(!isRecycled)

                getRenderedPageFile(page.index)?.takeIf(File::exists)?.let {
                    return@withLock it
                }

                if (!isPreload) {
                    page.status = Page.State.LoadPage
                }

                cacheDir.mkdirs()
                val outputFile = File(cacheDir, "${page.index}.png")
                renderPageToFile(page.index, outputFile)
                outputFile
            }

            page.stream = { renderedFile.inputStream() }
            page.status = Page.State.Ready
        } catch (e: Throwable) {
            if (!isPreload) {
                page.status = Page.State.Error(e)
            }
            throw e
        }
    }

    private fun getRenderedPageFile(pageIndex: Int): File? {
        val file = File(cacheDir, "$pageIndex.png")
        return file.takeIf(File::exists)
    }

    private fun renderPageToFile(
        pageIndex: Int,
        outputFile: File,
    ) {
        renderer.openPage(pageIndex).use { page ->
            val scale = (targetWidth.toFloat() / page.width.toFloat())
                .takeIf { it.isFinite() && it > 0f }
                ?: 1f
            var adjustedScale = scale
            var width = (page.width * adjustedScale).roundToInt().coerceAtLeast(1)
            var height = (page.height * adjustedScale).roundToInt().coerceAtLeast(1)
            val dimensionScale = (MAX_RENDER_DIMENSION.toFloat() / max(width, height).toFloat())
                .coerceAtMost(1f)
            if (dimensionScale < 1f) {
                adjustedScale *= dimensionScale
                width = (page.width * adjustedScale).roundToInt().coerceAtLeast(1)
                height = (page.height * adjustedScale).roundToInt().coerceAtLeast(1)
            }
            val matrix = Matrix().apply { setScale(adjustedScale, adjustedScale) }
            val bitmap = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)

            try {
                bitmap.eraseColor(Color.WHITE)
                page.render(bitmap, null, matrix, PdfRenderer.Page.RENDER_MODE_FOR_DISPLAY)
                outputFile.outputStream().use {
                    bitmap.compress(Bitmap.CompressFormat.PNG, 100, it)
                }
            } finally {
                bitmap.recycle()
            }
        }
    }

    private fun openFileDescriptor(
        context: Context,
        file: UniFile,
    ): ParcelFileDescriptor {
        val uri = file.uri
        return when (uri.scheme) {
            ContentResolver.SCHEME_FILE -> {
                val path = requireNotNull(uri.path) { "PDF file path is missing" }
                ParcelFileDescriptor.open(File(path), ParcelFileDescriptor.MODE_READ_ONLY)
            }
            else -> {
                context.contentResolver.openFileDescriptor(uri, "r")
                    ?: error("Unable to open PDF file descriptor")
            }
        }
    }
}

private const val MAX_RENDER_DIMENSION = 4096
