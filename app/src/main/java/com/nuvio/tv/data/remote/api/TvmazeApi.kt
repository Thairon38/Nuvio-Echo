
package com.nuvio.tv.data.remote.api

import com.squareup.moshi.Json
import com.squareup.moshi.JsonClass
import retrofit2.Response
import retrofit2.http.GET
import retrofit2.http.Path
import retrofit2.http.Query

/**
 * TVmaze API.
 *
 * Se utiliza para consultar fechas de emisión españolas
 * y estrenos web.
 * API pública, sin necesidad de API key.
 */
interface TvmazeApi {

    /**
     * Busca una serie mediante su identificador IMDb.
     */
    @GET("lookup/shows")
    suspend fun lookupShowByImdb(
        @Query("imdb") imdbId: String
    ): Response<TvmazeShow>

    /**
     * Programación de televisión lineal en España.
     */
    @GET("schedule")
    suspend fun getSpainTvSchedule(
        @Query("country") country: String = "ES",
        @Query("date") date: String
    ): Response<List<TvmazeScheduleEntry>>

    /**
     * Programación de plataformas y servicios web en España.
     */
    @GET("schedule/web")
    suspend fun getSpainWebSchedule(
        @Query("country") country: String = "ES",
        @Query("date") date: String
    ): Response<List<TvmazeScheduleEntry>>

    /**
     * Programación global de plataformas y servicios web.
     * Al ser null, Retrofit omite el parámetro country.
     */
    @GET("schedule/web")
    suspend fun getGlobalWebSchedule(
        @Query("country") country: String? = null,
        @Query("date") date: String
    ): Response<List<TvmazeScheduleEntry>>

    /**
     * Episodios conocidos de una serie.
     * Sus fechas no deben considerarse automáticamente
     * fechas españolas.
     */
    @GET("shows/{showId}/episodes")
    suspend fun getShowEpisodes(
        @Path("showId") showId: Int
    ): Response<List<TvmazeEpisode>>
}

@JsonClass(generateAdapter = true)
data class TvmazeShow(
    @Json(name = "id")
    val id: Int,

    @Json(name = "name")
    val name: String? = null,

    @Json(name = "language")
    val language: String? = null,

    @Json(name = "network")
    val network: TvmazeNetwork? = null,

    @Json(name = "webChannel")
    val webChannel: TvmazeNetwork? = null,

    @Json(name = "externals")
    val externals: TvmazeExternals? = null
)

@JsonClass(generateAdapter = true)
data class TvmazeScheduleEntry(
    @Json(name = "id")
    val id: Int? = null,

    @Json(name = "url")
    val url: String? = null,

    @Json(name = "name")
    val name: String? = null,

    @Json(name = "season")
    val season: Int? = null,

    @Json(name = "number")
    val number: Int? = null,

    @Json(name = "airdate")
    val airdate: String? = null,

    @Json(name = "airtime")
    val airtime: String? = null,

    @Json(name = "airstamp")
    val airstamp: String? = null,

    @Json(name = "runtime")
    val runtime: Int? = null,

    @Json(name = "summary")
    val summary: String? = null,

    @Json(name = "show")
    val show: TvmazeShow? = null
)

@JsonClass(generateAdapter = true)
data class TvmazeEpisode(
    @Json(name = "id")
    val id: Int,

    @Json(name = "name")
    val name: String? = null,

    @Json(name = "season")
    val season: Int? = null,

    @Json(name = "number")
    val number: Int? = null,

    @Json(name = "airdate")
    val airdate: String? = null,

    @Json(name = "airtime")
    val airtime: String? = null,

    @Json(name = "airstamp")
    val airstamp: String? = null,

    @Json(name = "runtime")
    val runtime: Int? = null,

    @Json(name = "summary")
    val summary: String? = null
)

@JsonClass(generateAdapter = true)
data class TvmazeNetwork(
    @Json(name = "id")
    val id: Int? = null,

    @Json(name = "name")
    val name: String? = null,

    @Json(name = "country")
    val country: TvmazeCountry? = null
)

@JsonClass(generateAdapter = true)
data class TvmazeCountry(
    @Json(name = "name")
    val name: String? = null,

    @Json(name = "code")
    val code: String? = null,

    @Json(name = "timezone")
    val timezone: String? = null
)

@JsonClass(generateAdapter = true)
data class TvmazeExternals(
    @Json(name = "tvrage")
    val tvrage: Int? = null,

    @Json(name = "thetvdb")
    val thetvdb: Int? = null,

    @Json(name = "imdb")
    val imdb: String? = null
)
