import json

import click

from fuckdl.objects import Title, Tracks
from fuckdl.services.BaseService import BaseService


class CTV(BaseService):
    """
    Service code for CTV Television Network's free streaming platform (https://ctv.ca).

    \b
    Authorization: None (Free Service)
    Security: UHD@-- HD@L3, doesn't care about releases.

    TODO: Movies are now supported

    fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["CTV"]
    # GEOFENCE = ["ca"]
    TITLE_RE = r"^(?:https?://(?:www\.)?ctv\.ca(?:/[a-z]{2})?/(?:movies|shows)/)?(?P<id>[a-z0-9-]+)"

    @staticmethod
    @click.command(name="CTV", short_help="https://ctv.ca")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return CTV(ctx, **kwargs)

    def __init__(self, ctx, movie, title):
        super().__init__(ctx)
        self.parse_title(ctx, title)
        self.movie = movie

        self.configure()

    def get_titles(self):
        title_information = self.session.post(
            url="https://api.ctv.ca/space-graphql/graphql",
            json={
                "operationName": "axisMedia",
                "variables": {"axisMediaId": self.title},
                "query": """
                query axisMedia($axisMediaId: ID!) {
                    contentData: axisMedia(id: $axisMediaId) {
                        id
                        axisId
                        title
                        originalSpokenLanguage
                        firstPlayableContent {
                            id
                            title
                            axisId
                            path
                            seasonNumber
                            episodeNumber
                        }
                        mediaType
                        firstAirYear
                        seasons {
                            title
                            id
                            seasonNumber
                        }
                    }
                }
                """,
            },
        ).json()["data"]["contentData"]
        titles = []
        if title_information["mediaType"] == "MOVIE" or self.movie:  # e.g. "tv-show" titles that are 1 episode "movies"
            return Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name=title_information["title"],
                year=title_information.get("firstAirYear"),
                original_lang=title_information["originalSpokenLanguage"],
                source=self.ALIASES[0],
                service_data=title_information["firstPlayableContent"]
            )

        titles = []
        for season in title_information["seasons"]:
            titles.extend(
                self.session.post(
                    url="https://api.ctv.ca/space-graphql/graphql",
                    json={
                        "operationName": "season",
                        "variables": {"seasonId": season["id"]},
                        "query": """
                    query season($seasonId: ID!) {
                        axisSeason(id: $seasonId) {
                            episodes {
                                axisId
                                title
                                contentType
                                seasonNumber
                                episodeNumber
                                axisPlaybackLanguages {
                                    language
                                }
                            }
                        }
                    }
                    """,
                    },
                ).json()["data"]["axisSeason"]["episodes"]
            )
        return [
            Title(
                id_=self.title,
                type_=Title.Types.TV,
                name=title_information["title"],
                year=None,  # TODO: Implement year
                season=x.get("seasonNumber"),
                episode=x.get("episodeNumber"),
                episode_name=x.get("title"),
                original_lang=title_information["originalSpokenLanguage"],
                source=self.ALIASES[0],
                service_data=x,
            )
            for x in titles
        ]

    def get_tracks(self, title: Title):
        # platform_type = "ctvthrowback_hub" if title.type == Title.Types.TV else "ctvmovies_hub"
        platform_type = self.get_title_info(title)
        self.log.warning(f"Platform_type: {platform_type}")

        response = self.session.get(
            url=self.config["endpoints"]["content_packages"].format(
                platform_type=platform_type,
                title_id=title.service_data["axisId"]
            ),
            params={"$lang": "en"},
        )
        try:
            package_id = response.json()["Items"][0]["Id"]
        except Exception:
            raise self.log.exit(response.text)

        mpd_url = self.config["endpoints"]["manifest"].format(
            platform_type=platform_type,
            title_id=title.service_data["axisId"],
            package_id=package_id
        )
        r = self.session.get(mpd_url, params={"filter": 25})
        try:
            mpd_data = r.json()
        except json.JSONDecodeError:
            # awesome, probably no error, should be an MPD
            mpd_data = r.text
        else:
            if "ErrorCode" in mpd_data:
                raise Exception(
                    "CTV reported an error when obtaining the MPD Manifest.\n"
                    + f"{mpd_data['Message']} ({mpd_data['ErrorCode']})"
                )

        tracks = Tracks.from_mpd(data=mpd_data, url=mpd_url, source=self.ALIASES[0])

        # Convert PSSH Box objects to base64 strings for compatibility with dl.py
        from fuckdl.vendor.pymp4.parser import Box as Mp4Box
        for track in tracks:
            if hasattr(track, 'pssh') and track.pssh and not isinstance(track.pssh, (str, bytes)):
                try:
                    import base64
                    track.pssh = base64.b64encode(Mp4Box.build(track.pssh)).decode()
                except Exception:
                    pass

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **kwargs):
        # TODO: Hardcode the certificate
        return self.license(**kwargs)

    def license(self, challenge, **_):
        return self.session.post(
            url=self.config["endpoints"]["license"],
            data=challenge,  # expects bytes
        ).content

    # Service specific functions

    def configure(self):
        print("Fetching real title id...")
        axis_id = self.get_axis_id(f"/tv-shows/{self.title}") or self.get_axis_id(f"/movies/{self.title}")
        if axis_id:
            self.title = axis_id
        else:
            # raise self.log.exit(f" - Could not obtain the Axis ID for {self.title!r}, are you sure it's right?")
            response = self.session.post(
                url="https://api.ctv.ca/space-graphql/graphql",
                json={
                    "operationName": "resolvePath",
                    "variables": {"path": f"/shows/{self.title}"},
                    "query": """
                    query resolvePath($path: String!) {
                        resolvedPath(path: $path) {
                            lastSegment {
                                content {
                                    id
                                }
                            }
                        }
                    }
                    """,
                },
            )
            response_json = response.json()
            try:
                self.title = response_json["data"]["resolvedPath"]["lastSegment"]["content"]["id"]
            except TypeError:
                raise self.log.exit(response_json)

        print(f"Got axis title id: {self.title}")

    def get_title_info(self, title: int):
        json_data = {
            "operationName": "axisContent",
            "variables": {
                "id": f"contentid/axis-content-{title.service_data['axisId']}",
                "subscriptions": [],
                "maturity": "ADULT",
                "language": "ENGLISH",
                "authenticationState": "AUTH",
                "playbackLanguage": "ENGLISH",
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "d6e75de9b5836cd6305c98c8d2411e336f59eb12f095a61f71d454f3fae2ecda",
                },
            },
            "query": "query axisContent($id: ID!, $subscriptions: [Subscription]!, $maturity: Maturity!, $language: Language!, $authenticationState: AuthenticationState!, $playbackLanguage: PlaybackLanguage!) @uaContext(subscriptions: $subscriptions, maturity: $maturity, language: $language, authenticationState: $authenticationState, playbackLanguage: $playbackLanguage) {\n  axisContent(id: $id) {\n    axisId\n    id\n    path\n    title\n    duration\n    agvotCode\n    description\n    episodeNumber\n    seasonNumber\n    pathSegment\n    genres {\n      name\n      __typename\n    }\n    axisMedia {\n      heroBrandLogoId\n      id\n      title\n      __typename\n    }\n    adUnit {\n      ...AxisAdUnitData\n      __typename\n    }\n    authConstraints {\n      ...AuthConstraintsData\n      __typename\n    }\n    axisPlaybackLanguages {\n      ...AxisPlaybackData\n      __typename\n    }\n    originalSpokenLanguage\n    ogFields {\n      ogDescription\n      ogImages {\n        url\n        __typename\n      }\n      ogTitle\n      __typename\n    }\n    playbackMetadata {\n      indicator\n      languages {\n        languageCode\n        languageDisplayName\n        __typename\n      }\n      __typename\n    }\n    seoFields {\n      seoDescription\n      seoTitle\n      seoKeywords\n      canonicalUrl\n      __typename\n    }\n    badges {\n      title\n      label\n      __typename\n    }\n    posterImages: images(formats: POSTER) {\n      url\n      __typename\n    }\n    broadcastDate\n    expiresOn\n    startsOn\n    keywords\n    videoPageLayout {\n      __typename\n      ... on Rotator {\n        id\n        config {\n          ...RotatorConfigData\n          __typename\n        }\n        __typename\n      }\n    }\n    __typename\n  }\n}\n\nfragment AxisAdUnitData on AxisAdUnit {\n  adultAudience\n  heroBrand\n  pageType\n  product\n  revShare\n  title\n  analyticsTitle\n  keyValue {\n    webformType\n    adTarget\n    contentType\n    mediaType\n    pageTitle\n    revShare\n    subType\n    __typename\n  }\n  __typename\n}\n\nfragment RotatorConfigData on RotatorConfig {\n  displayTitle\n  displayTotalItemCount\n  displayDots\n  style\n  imageFormat\n  lightbox\n  carousel\n  titleLinkMode\n  maxItems\n  disableBadges\n  customTitleLink {\n    ...LinkData\n    __typename\n  }\n  hideMediaTitle\n  __typename\n}\n\nfragment LinkData on Link {\n  buttonStyle\n  urlParameters\n  renderAs\n  linkType\n  linkLabel\n  longLinkLabel\n  linkTarget\n  userMgmtLinkType\n  url\n  id\n  showLinkLabel\n  internalContent {\n    title\n    __typename\n    ... on AxisContent {\n      axisId\n      authConstraints {\n        ...AuthConstraintsData\n        __typename\n      }\n      agvotCode\n      __typename\n    }\n    ... on AceWebContent {\n      path\n      pathSegment\n      __typename\n    }\n    ... on Section {\n      containerType\n      path\n      __typename\n    }\n    ... on AxisObject {\n      axisId\n      title\n      __typename\n    }\n    ... on TabItem {\n      sectionPath\n      __typename\n    }\n  }\n  hoverImage {\n    title\n    imageType\n    url\n    __typename\n  }\n  image {\n    id\n    width\n    height\n    title\n    url\n    altText\n    __typename\n  }\n  bannerImages {\n    breakPoint\n    image {\n      id\n      title\n      url\n      altText\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment AuthConstraintsData on AuthConstraint {\n  authRequired\n  packageName\n  endDate\n  language\n  startDate\n  subscriptionName\n  __typename\n}\n\nfragment AxisPlaybackData on AxisPlayback {\n  destinationCode\n  language\n  duration\n  playbackIndicators\n  partOfMultiLanguagePlayback\n  __typename\n}\n",
        }

        response = self.session.post("https://www.ctv.ca/space-graphql/apq/graphql", json=json_data)
        response_json = response.json()

        try:
            return response_json["data"]["axisContent"]["authConstraints"][0]["packageName"]
        except TypeError:
            raise self.log.exit("Unable to get Platform_type")

    def get_axis_id(self, path):
        res = self.session.post(
            url="https://api.ctv.ca/space-graphql/graphql",
            json={
                "operationName": "resolvePath",
                "variables": {
                    "path": path
                },
                "query": """
                query resolvePath($path: String!) {
                    resolvedPath(path: $path) {
                        lastSegment {
                            content {
                                id
                            }
                        }
                    }
                }
                """
            }
        ).json()
        if "errors" in res:
            if res["errors"][0]["extensions"]["code"] == "NOT_FOUND":
                return None
            raise ValueError("Unknown error has occurred when trying to obtain the Axis ID for: " + path)
        return res["data"]["resolvedPath"]["lastSegment"]["content"]["id"]
