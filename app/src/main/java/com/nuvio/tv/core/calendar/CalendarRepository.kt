
package com.nuvio.tv.core.calendar

import android.content.Context
import android.util.Log
import com.nuvio.tv.core.network.NetworkResult
import com.nuvio.tv.core.profile.ProfileManager
import com.nuvio.tv.core.tmdb.TmdbService
import com.nuvio.tv.core.util.parseEpisodeReleaseLocalDate
import com.nuvio.tv.data.local.TmdbSettingsDataStore
import com.nuvio.tv.data.remote.api.TmdbApi
import com.nuvio.tv.data.remote.api.TmdbScheduleEpisode
import com.nuvio.tv.data.remote.api.TvmazeApi
import com.nuvio.tv.data.remote.api.TvmazeScheduleEntry
import com.nuvio.tv.domain.model.Meta
import com.nuvio.tv.domain.repository.LibraryRepository
import com.nuvio.tv.domain.repository.MetaRepository
import com.nuvio.tv.domain.repository.WatchProgressRepository
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import javax.inject.Inject
import javax.inject.Singleton

/**
 * The Calendar: when the next episodes of the shows the active profile follows come out,
 * and the "Remind me" reminders that show on Home on the day.
 */
@Singleton
class CalendarRepository @Inject constructor(
    @ApplicationContext context: Context,
    private val tmdbApi: TmdbApi,
    private val tvmazeApi: TvmazeApi,
    private val tmdbService: TmdbService,
    private val tmdbSettingsDataStore: TmdbSettingsDataStore,
    private val metaRepository: MetaRepository,
    private val libraryRepository: LibraryRepository,
    private val watchProgressRepository: WatchProgressRepository,
    private val profileManager: ProfileManager
) {

    data class Snapshot(
        val shows: List<CalendarShow> = emptyList(),
        val followed: Int = 0,
        val reminders: Map<String, CalendarReminder> = emptyMap(),
        val refreshing: Boolean = false,
        val loaded: Boolean = false
    )

    private val store = CalendarStore(context)
    private val lock = Mutex()
    private val refreshLock = Mutex()

    // Caché compartida de programación de TVmaze.
    private val tvmazeScheduleLock = Mutex()
    private var tvmazeScheduleFetchedAt = 0L
    private var tvmazeSchedule = emptyList<RegionalScheduleEntry>()

    private var profileId = -1
    private val shows = LinkedHashMap<String, CalendarShow>()
    private val reminders = LinkedHashMap<String, CalendarReminder>()
    private var followedIds: List<String> = emptyList()

    private val _state = MutableStateFlow(Snapshot())
    val state: StateFlow<Snapshot> = _state.asStateFlow()

    private val hasTmdb: Boolean
        get() = com.nuvio.tv.core.apikeys.ApiKeys.tmdb.isNotBlank()

    private data class RegionalScheduleEntry(
        val episode: TvmazeScheduleEntry,
        val source: CalendarDateSource
    )

    private data class ScheduleFetch(
        val success: Boolean,
        val entries: List<RegionalScheduleEntry>
    )

    private data class Followed(
        val id: String,
        val title: String,
        val poster: String?,
        val background: String?,
        val addonBaseUrl: String?,
        val imdbId: String?,
        val tmdbId: Int?
    )

    /** Lists followed shows, displays cached answers and refreshes stale entries. */
    suspend fun refresh(force: Boolean = false) = withContext(Dispatchers.IO) {
        if (!refreshLock.tryLock()) return@withContext

        try {
            ensureProfile()
            val followed = followed()

            lock.withLock {
                followedIds = followed.map { it.id }

                for (f in followed) {
                    val cached = shows[f.id]

                    shows[f.id] = cached?.copy(
                        title = cached.title.ifBlank { f.title },
                        poster = cached.poster ?: f.poster,
                        backdrop = cached.backdrop ?: f.background,
                        addonBaseUrl = cached.addonBaseUrl ?: f.addonBaseUrl
                    ) ?: placeholder(f)
                }
            }

            publish(refreshing = true, loaded = true)

            val now = System.currentTimeMillis()
            val stale = followed.filter { f ->
                force || isStale(shows[f.id], now)
            }

            fetchAll(stale)

            lock.withLock {
                store.writeShows(profileId, shows.values)
            }
        } finally {
            publish(refreshing = false, loaded = true)
            refreshLock.unlock()
        }
    }

    /** Turns the reminder for a show on or off. */
    suspend fun toggleReminder(showId: String) = withContext(Dispatchers.IO) {
        ensureProfile()

        lock.withLock {
            if (reminders.remove(showId) == null) {
                val show = shows[showId] ?: return@withLock

                if (show.upcoming(LocalDate.now()) == null) {
                    return@withLock
                }

                reminders[showId] = CalendarReminder(
                    showId,
                    show.title,
                    LocalDate.now(),
                    notifiedThrough = null
                )
            }

            store.writeReminders(profileId, reminders.values)
        }

        publish()
    }

    /** Reminded shows whose episode is out and wasn't shown yet. */
    suspend fun dueReminders(): List<CalendarDue> = withContext(Dispatchers.IO) {
        ensureProfile()

        val pending = lock.withLock {
            reminders.values.toList()
        }

        if (pending.isEmpty()) return@withContext emptyList()

        val now = System.currentTimeMillis()

        val stale = pending.mapNotNull { r ->
            val show = lock.withLock {
                shows[r.showId]
            }

            if (show != null && !isStale(show, now)) {
                null
            } else {
                Followed(
                    id = r.showId,
                    title = show?.title ?: r.title,
                    poster = show?.poster,
                    background = show?.backdrop,
                    addonBaseUrl = show?.addonBaseUrl,
                    imdbId = r.showId.takeIf { it.startsWith("tt") },
                    tmdbId = show?.tmdbId
                )
            }
        }

        if (stale.isNotEmpty()) {
            fetchAll(stale)

            lock.withLock {
                store.writeShows(profileId, shows.values)
            }
        }

        val today = LocalDate.now()

        lock.withLock {
            pending.mapNotNull { r ->
                val show = shows[r.showId] ?: return@mapNotNull null

                val episode = listOfNotNull(show.next, show.last)
                    .filter { ep ->
                        val date = ep.airDate ?: return@filter false

                        !date.isAfter(today) &&
                            !date.isBefore(r.since) &&
                            (r.notifiedThrough == null ||
                                date.isAfter(r.notifiedThrough))
                    }
                    .maxByOrNull { it.airDate!! }
                    ?: return@mapNotNull null

                CalendarDue(show, episode)
            }
        }
    }

    /** Followed shows whose next episode airs today, using the cache only. */
    suspend fun airingToday(): List<CalendarDue> = withContext(Dispatchers.IO) {
        ensureProfile()

        val today = LocalDate.now()

        lock.withLock {
            shows.values.mapNotNull { show ->
                if (show.id in reminders) return@mapNotNull null

                val next = show.next?.takeIf {
                    it.airDate == today
                } ?: return@mapNotNull null

                CalendarDue(show, next)
            }
        }
    }

    /** Marks reminders already shown on Home. */
    suspend fun markNotified(due: List<CalendarDue>) = withContext(Dispatchers.IO) {
        if (due.isEmpty()) return@withContext

        lock.withLock {
            for (d in due) {
                val r = reminders[d.show.id] ?: continue
                val date = d.episode.airDate ?: continue

                if (r.notifiedThrough == null ||
                    date.isAfter(r.notifiedThrough)
                ) {
                    reminders[d.show.id] = r.copy(
                        notifiedThrough = date
                    )
                }
            }

            store.writeReminders(profileId, reminders.values)
        }

        publish()
    }

    private suspend fun ensureProfile() {
        val active = profileManager.activeProfileId.value

        lock.withLock {
            if (active == profileId) return

            profileId = active

            shows.clear()
            shows.putAll(store.readShows(active))

            reminders.clear()
            reminders.putAll(store.readReminders(active))

            followedIds = emptyList()
        }

        _state.value = Snapshot(
            reminders = lock.withLock {
                reminders.toMap()
            }
        )
    }

    private suspend fun publish(
        refreshing: Boolean = _state.value.refreshing,
        loaded: Boolean = _state.value.loaded
    ) {
        val snapshot = lock.withLock {
            Snapshot(
                shows = followedIds.mapNotNull { shows[it] },
                followed = followedIds.size,
                reminders = reminders.toMap(),
                refreshing = refreshing,
                loaded = loaded
            )
        }

        _state.value = snapshot
    }

    /** Series in the library plus series watched lately, library first, each once. */
    private suspend fun followed(): List<Followed> {
        val out = LinkedHashMap<String, Followed>()

        val library = withTimeoutOrNull(LIST_TIMEOUT_MS) {
            libraryRepository.libraryItems.first()
        }.orEmpty()

        for (entry in library) {
            if (!isSeries(entry.type)) continue

            val key = entry.imdbId?.takeIf {
                it.startsWith("tt")
            } ?: entry.id

            if (key in out) continue

            out[key] = Followed(
                id = key,
                title = entry.name,
                poster = entry.poster,
                background = entry.background,
                addonBaseUrl = entry.addonBaseUrl,
                imdbId = entry.imdbId,
                tmdbId = entry.tmdbId
            )
        }

        val cutoff = System.currentTimeMillis() - WATCHED_WITHIN_MS

        val progress = withTimeoutOrNull(LIST_TIMEOUT_MS) {
            watchProgressRepository.allProgress.first()
        }.orEmpty()

        for (p in progress.sortedByDescending { it.lastWatched }) {
            if (!isSeries(p.contentType) || p.lastWatched < cutoff) continue

            if (p.contentId in out ||
                runCatching {
                    watchProgressRepository.isDroppedShow(p.contentId)
                }.getOrDefault(false)
            ) {
                continue
            }

            out[p.contentId] = Followed(
                id = p.contentId,
                title = p.name,
                poster = p.poster,
                background = p.backdrop,
                addonBaseUrl = p.addonBaseUrl,
                imdbId = p.contentId.takeIf { it.startsWith("tt") },
                tmdbId = null
            )
        }

        return out.values.take(MAX_SHOWS)
    }

    private fun isSeries(type: String?) =
        type.equals("series", ignoreCase = true) ||
            type.equals("tv", ignoreCase = true)

    private fun isStale(show: CalendarShow?, now: Long) =
        show == null || now - show.fetchedAt > STALE_MS

    private fun placeholder(f: Followed) = CalendarShow(
        id = f.id,
        title = f.title,
        poster = f.poster,
        backdrop = f.background,
        status = CalendarStatus.UNKNOWN,
        next = null,
        last = null,
        network = null,
        seasons = null,
        genre = null,
        addonBaseUrl = f.addonBaseUrl,
        tmdbId = f.tmdbId,
        fetchedAt = 0L
    )

    /**
     * Obtiene programación de TV española, plataformas españolas
     * y plataformas globales.
     *
     * Las consultas se agrupan por fecha y se guardan en caché
     * durante 60 minutos cuando todas terminan correctamente.
     */
    private suspend fun fetchRegionalSchedule(): ScheduleFetch =
        tvmazeScheduleLock.withLock {
            val now = System.currentTimeMillis()

            if (now - tvmazeScheduleFetchedAt < TVMAZE_CACHE_MS) {
                return@withLock ScheduleFetch(
                    success = true,
                    entries = tvmazeSchedule
                )
            }

            val today = LocalDate.now(ZoneId.of("Europe/Madrid"))

            val dates = (0 until TVMAZE_DAYS).map {
                today.plusDays(it.toLong())
            }

            val gate = Semaphore(TVMAZE_PARALLEL)

            val results = coroutineScope {
                dates.map { date ->
                    async {
                        gate.withPermit {
                            val tv = try {
                                tvmazeApi.getSpainTvSchedule(
                                    date = date.toString()
                                )
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                Log.w(TAG, "TVmaze TV error: $date", e)
                                null
                            }

                            val spainWeb = try {
                                tvmazeApi.getSpainWebSchedule(
                                    date = date.toString()
                                )
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                Log.w(TAG, "TVmaze Spain web error: $date", e)
                                null
                            }

                            val globalWeb = try {
                                tvmazeApi.getGlobalWebSchedule(
                                    date = date.toString()
                                )
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                Log.w(TAG, "TVmaze global web error: $date", e)
                                null
                            }

                            val entries = buildList {
                                if (tv?.isSuccessful == true) {
                                    tv.body().orEmpty().forEach {
                                        add(
                                            RegionalScheduleEntry(
                                                episode = it,
                                                source = CalendarDateSource.SPAIN_TV
                                            )
                                        )
                                    }
                                }

                                if (spainWeb?.isSuccessful == true) {
                                    spainWeb.body().orEmpty().forEach {
                                        add(
                                            RegionalScheduleEntry(
                                                episode = it,
                                                source = CalendarDateSource.SPAIN_STREAMING
                                            )
                                        )
                                    }
                                }

                                if (globalWeb?.isSuccessful == true) {
                                    globalWeb.body().orEmpty().forEach {
                                        add(
                                            RegionalScheduleEntry(
                                                episode = it,
                                                source = CalendarDateSource.GENERAL
                                            )
                                        )
                                    }
                                }
                            }

                            val success =
                                tv?.isSuccessful == true &&
                                spainWeb?.isSuccessful == true &&
                                globalWeb?.isSuccessful == true

                            ScheduleFetch(
                                success = success,
                                entries = entries
                            )
                        }
                    }
                }.awaitAll()
            }

            val allSuccessful = results.all {
                it.success
            }

            val entries = results.flatMap {
                it.entries
            }

            if (allSuccessful) {
                tvmazeSchedule = entries
                tvmazeScheduleFetchedAt = now

                Log.i(
                    TAG,
                    "TVmaze: programación española y global cargada: " +
                        "${entries.size} episodios"
                )
            } else {
                Log.w(
                    TAG,
                    "TVmaze: programación incompleta; se conserva la caché anterior"
                )
            }

            ScheduleFetch(
                success = allSuccessful,
                entries = entries
            )
        }

    /**
     * Descarga la programación una sola vez y la comparte
     * entre todas las series seguidas.
     */
    private suspend fun fetchAll(followed: List<Followed>) {
        if (followed.isEmpty()) return

        val started = System.currentTimeMillis()
        val gate = Semaphore(PARALLEL)

        val fromTmdb = AtomicInteger(0)
        val fromAddons = AtomicInteger(0)
        val failed = AtomicInteger(0)
        val lastPublish = AtomicLong(0L)

        val regional = try {
            if (followed.any {
                    (it.imdbId ?: it.id).startsWith("tt")
                }
            ) {
                fetchRegionalSchedule()
            } else {
                ScheduleFetch(
                    success = false,
                    entries = emptyList()
                )
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Log.w(TAG, "Error cargando programación regional", e)
            ScheduleFetch(
                success = false,
                entries = emptyList()
            )
        }

        // Prioridad: televisión española, plataformas españolas,
        // plataformas globales.
        val regionalIndex = regional.entries
            .mapNotNull { entry ->
                val imdb = entry.episode.show?.externals?.imdb
                    ?.takeIf { it.startsWith("tt") }
                    ?: return@mapNotNull null

                val season = entry.episode.season
                    ?: return@mapNotNull null

                val episode = entry.episode.number
                    ?: return@mapNotNull null

                val date = entry.episode.airdate
                    ?: return@mapNotNull null

                val parsedDate = runCatching {
                    LocalDate.parse(date)
                }.getOrNull() ?: return@mapNotNull null

                regionalKey(imdb, season, episode) to entry
            }
            .groupBy(
                keySelector = { it.first },
                valueTransform = { it.second }
            )
            .mapValues { (_, entries) ->
                entries.minByOrNull {
                    when (it.source) {
                        CalendarDateSource.SPAIN_TV -> 0
                        CalendarDateSource.SPAIN_STREAMING -> 1
                        CalendarDateSource.GENERAL -> 2
                        else -> 3
                    }
                }!!
            }

        coroutineScope {
            followed.map { f ->
                async {
                    gate.withPermit {
                        val cached = lock.withLock {
                            shows[f.id]
                        }

                        val result = try {
                            val tmdb = fetchTmdb(f, cached)

                            if (tmdb != null) {
                                fromTmdb.incrementAndGet()
                                applyRegionalDates(
                                    tmdb,
                                    f,
                                    regionalIndex
                                )
                            } else {
                                val addon = fetchAddons(f, cached)

                                if (addon != null) {
                                    fromAddons.incrementAndGet()
                                    applyRegionalDates(
                                        addon,
                                        f,
                                        regionalIndex
                                    )
                                } else {
                                    null
                                }
                            }
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            Log.w(TAG, "Error actualizando ${f.title}", e)
                            null
                        }

                        val now = System.currentTimeMillis()

                        val updated = result ?: run {
                            failed.incrementAndGet()

                            (cached ?: placeholder(f)).copy(
                                fetchedAt = now - STALE_MS + RETRY_MS
                            )
                        }

                        lock.withLock {
                            shows[f.id] = updated
                        }

                        val previous = lastPublish.get()

                        if (now - previous > PUBLISH_EVERY_MS &&
                            lastPublish.compareAndSet(previous, now)
                        ) {
                            publish()
                        }
                    }
                }
            }.awaitAll()
        }

        Log.i(
            TAG,
            "Actualizadas ${followed.size} series en " +
                "${(System.currentTimeMillis() - started) / 1000.0} s: " +
                "${fromTmdb.get()} TMDB, " +
                "${fromAddons.get()} addons, " +
                "${failed.get()} sin respuesta; " +
                "${regionalIndex.size} episodios regionales"
        )
    }

    private fun regionalKey(
        imdbId: String,
        season: Int,
        episode: Int
    ): String =
        "${imdbId.trim().lowercase()}|$season|$episode"

    /**
     * Aplica la fecha regional si coincide exactamente
     * IMDb + temporada + episodio.
     *
     * La entrada elegida ya respeta la prioridad española.
     * Si no hay coincidencia española, puede usar la global.
     */
    private fun applyRegionalDates(
        show: CalendarShow,
        followed: Followed,
        regionalIndex: Map<String, RegionalScheduleEntry>
    ): CalendarShow {
        val imdb = followed.imdbId
            ?.takeIf { it.startsWith("tt") }
            ?: followed.id.takeIf { it.startsWith("tt") }
            ?: return show

        fun apply(
            episode: CalendarEpisode?
        ): CalendarEpisode? {
            episode ?: return null

            val key = regionalKey(
                imdb,
                episode.season,
                episode.episode
            )

            val regional = regionalIndex[key]
                ?: return episode

            val date = regional.episode.airdate
                ?.let {
                    runCatching { LocalDate.parse(it) }.getOrNull()
                }
                ?: return episode

            return episode.copy(
                airDate = date,
                dateSource = regional.source
            )
        }

        val next = apply(show.next)
        val last = apply(show.last)

        val matchedEpisode = listOfNotNull(next, last)
            .firstOrNull { episode ->
                regionalIndex.containsKey(
                    regionalKey(
                        imdb,
                        episode.season,
                        episode.episode
                    )
                )
            }

        val matchedEntry = matchedEpisode?.let { episode ->
            regionalIndex[
                regionalKey(
                    imdb,
                    episode.season,
                    episode.episode
                )
            ]
        }

        val regionalNetwork = matchedEntry?.let { entry ->
            when (entry.source) {
                CalendarDateSource.SPAIN_TV ->
                    entry.episode.show?.network?.name

                CalendarDateSource.SPAIN_STREAMING,
                CalendarDateSource.GENERAL ->
                    entry.episode.show?.webChannel?.name

                else -> null
            }
        }

        return show.copy(
            next = next,
            last = last,
            network = regionalNetwork
                ?.takeIf { it.isNotBlank() }
                ?: show.network,
            backdrop = next?.still ?: show.backdrop
        )
    }

    private suspend fun fetchTmdb(
        f: Followed,
        cached: CalendarShow?
    ): CalendarShow? {
        if (!hasTmdb) return null

        val tmdbId = cached?.tmdbId ?: f.tmdbId
            ?: tmdbService.ensureTmdbId(
                f.id,
                "tv",
                f.imdbId
            )?.toIntOrNull()
            ?: return null

        val language = tmdbSettingsDataStore.settings.value.language

        val body = tmdbApi.getTvSchedule(
            tmdbId,
            com.nuvio.tv.core.apikeys.ApiKeys.tmdb,
            language
        ).body() ?: return null

        val today = LocalDate.now()

        val next = body.nextEpisodeToAir?.toEpisode()
        val last = body.lastEpisodeToAir?.toEpisode()

        val upcoming = next?.takeIf {
            it.airDate == null || !it.airDate.isBefore(today)
        }

        val aired = if (next != null && upcoming == null) {
            next
        } else {
            last
        }

        return CalendarShow(
            id = f.id,
            title = body.name?.takeIf { it.isNotBlank() }
                ?: cached?.title
                ?: f.title,
            poster = body.posterPath?.let {
                "$IMAGE_BASE/w342$it"
            } ?: cached?.poster ?: f.poster,
            backdrop = (upcoming ?: aired)?.still
                ?: body.backdropPath?.let {
                    "$IMAGE_BASE/w780$it"
                }
                ?: cached?.backdrop
                ?: f.background,
            status = CalendarStatus.from(body.status),
            next = upcoming,
            last = aired,
            network = body.networks
                ?.firstOrNull { !it.name.isNullOrBlank() }
                ?.name,
            seasons = body.numberOfSeasons,
            genre = body.genres?.firstOrNull()?.name,
            addonBaseUrl = cached?.addonBaseUrl ?: f.addonBaseUrl,
            tmdbId = tmdbId,
            fetchedAt = System.currentTimeMillis()
        ).let { show ->
            val runtime = body.episodeRunTime
                ?.firstOrNull { it > 0 }

            if (runtime == null) {
                show
            } else {
                show.copy(
                    next = show.next?.let {
                        if (it.runtimeMinutes == null) {
                            it.copy(runtimeMinutes = runtime)
                        } else {
                            it
                        }
                    },
                    last = show.last?.let {
                        if (it.runtimeMinutes == null) {
                            it.copy(runtimeMinutes = runtime)
                        } else {
                            it
                        }
                    }
                )
            }
        }
    }

    private fun TmdbScheduleEpisode.toEpisode(): CalendarEpisode? {
        val season = seasonNumber ?: return null
        val number = episodeNumber ?: return null

        return CalendarEpisode(
            season = season,
            episode = number,
            name = name?.takeIf { it.isNotBlank() },
            airDate = airDate?.let {
                runCatching { LocalDate.parse(it) }.getOrNull()
            },
            overview = overview?.takeIf { it.isNotBlank() },
            runtimeMinutes = runtime?.takeIf { it > 0 },
            still = stillPath?.let {
                "$IMAGE_BASE/w780$it"
            },
            type = episodeType,
            dateSource = CalendarDateSource.GENERAL
        )
    }

    /** Without TMDB: use addon metadata as the final fallback. */
    private suspend fun fetchAddons(
        f: Followed,
        cached: CalendarShow?
    ): CalendarShow? {
        val meta = withTimeoutOrNull(ADDON_TIMEOUT_MS) {
            metaRepository.getMetaFromAllAddons(
                "series",
                f.id,
                f.addonBaseUrl
            ).first {
                it !is NetworkResult.Loading
            }
        }

        val data = (meta as? NetworkResult.Success<Meta>)?.data
            ?: return null

        val today = LocalDate.now()

        val episodes = data.videos.mapNotNull { v ->
            val season = v.season?.takeIf { it > 0 }
                ?: return@mapNotNull null

            val number = v.episode
                ?: return@mapNotNull null

            val date = parseEpisodeReleaseLocalDate(v.released)
                ?: return@mapNotNull null

            CalendarEpisode(
                season = season,
                episode = number,
                name = v.title.takeIf { it.isNotBlank() },
                airDate = date,
                overview = v.overview?.takeIf { it.isNotBlank() },
                runtimeMinutes = v.runtime,
                still = v.thumbnail,
                type = null,
                dateSource = CalendarDateSource.ADDON_UNVERIFIED
            )
        }

        val next = episodes
            .filter { !it.airDate!!.isBefore(today) }
            .minByOrNull { it.airDate!! }

        val last = episodes
            .filter { it.airDate!!.isBefore(today) }
            .maxByOrNull { it.airDate!! }

        return CalendarShow(
            id = f.id,
            title = data.name.ifBlank {
                cached?.title ?: f.title
            },
            poster = data.poster ?: cached?.poster ?: f.poster,
            backdrop = (next ?: last)?.still
                ?: data.background
                ?: cached?.backdrop
                ?: f.background,
            status = CalendarStatus.from(data.status),
            next = next,
            last = last,
            network = data.networks.firstOrNull()?.name,
            seasons = episodes.maxOfOrNull { it.season },
            genre = data.genres.firstOrNull(),
            addonBaseUrl = cached?.addonBaseUrl ?: f.addonBaseUrl,
            tmdbId = cached?.tmdbId,
            fetchedAt = System.currentTimeMillis()
        )
    }

    private companion object {
        const val TAG = "Calendar"
        const val IMAGE_BASE = "https://image.tmdb.org/t/p"

        const val STALE_MS = 12 * 60 * 60 * 1000L
        const val RETRY_MS = 60 * 60 * 1000L
        const val WATCHED_WITHIN_MS =
            180L * 24 * 60 * 60 * 1000L

        const val MAX_SHOWS = 150
        const val PARALLEL = 4
        const val PUBLISH_EVERY_MS = 400L
        const val LIST_TIMEOUT_MS = 5_000L
        const val ADDON_TIMEOUT_MS = 15_000L

        const val TVMAZE_DAYS = 14
        const val TVMAZE_PARALLEL = 2
        const val TVMAZE_CACHE_MS = 60L * 60L * 1000L
    }
}
