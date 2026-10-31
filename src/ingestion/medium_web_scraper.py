import time
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from retry import retry

from utils.logger import logger
from utils.utils import (
    MediumGraphQLError,
    create_requests_session,
    get_extracted_at,
    get_extracted_at_epoch,
    get_extraction_id,
    get_json_content,
    medium_post_graphql_request,
    save_to_landing_zone,
)


class MediumWebScraper:
    def __init__(self, tags: List[str], lookback_days: int):
        self.lookback_days = lookback_days
        # Medium caps tag feed pages at 20 posts regardless of the requested page size.
        self.page_size = 20
        # Safety cap on pages per tag (~500 posts), well above a week of the busiest tag.
        self.max_pages = 25
        self.tags = tags

    def run(self) -> None:
        failed_tags = []
        for tag in self.tags:
            logger.info(
                f"Scraping blogs with tag '{tag}' from the last {self.lookback_days} days..."
            )

            # Medium intermittently blocks the scrape for a given tag. Treat a failed tag
            # as non-fatal so one blocked tag does not abort the whole pipeline (the
            # GitHub Discussions data is already gathered by this point).
            try:
                scraped_data = self.scrape_blogs(tag=tag)
            except MediumGraphQLError:
                logger.exception(f"Skipping tag '{tag}' after Medium GraphQL failure.")
                failed_tags.append(tag)
                continue

            save_to_landing_zone(
                data=[dict(t) for t in {tuple(d.items()) for d in scraped_data}],
                file_name=f"domain=medium_blogs/tag={tag}/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
            )

        # Every tag failing means Medium is blocking the runner outright. Fail loudly rather
        # than let the weekly email silently go out with stale blogs.
        if failed_tags and len(failed_tags) == len(self.tags):
            raise MediumGraphQLError(f"Medium GraphQL failed for every tag: {failed_tags}.")
        if failed_tags:
            logger.warning(f"Medium GraphQL failed for {len(failed_tags)} tag(s): {failed_tags}.")

        author_data = self.scrape_authors(extraction_id=get_extraction_id())
        save_to_landing_zone(
            data=author_data,
            file_name=f"domain=medium_authors/schema_version=2/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
        )

    # Has a habit of failing so keeping the retry logic tight
    def scrape_authors(self, extraction_id: str) -> List[Dict[str, object]]:
        @retry(tries=1, delay=3)
        def medium_scrape_authors(author_url: str) -> Dict[str, object]:
            time.sleep(1)  # To avoid rate limit detection
            logger.info(f"Scraping author URL: {author_url}...")

            page = create_requests_session().get(f"{author_url}")
            soup = BeautifulSoup(page.text, "html.parser")

            # Fails very often so using placeholder to avoid failing the entire pipeline.
            try:
                author_name = [
                    x.text for x in soup.find_all("h2") if str(x).find("author-name") > 0
                ][0]
            except Exception:
                author_name = "Unable to parse"

            num_followers_base = list(
                {
                    x.text
                    for x in soup.find_all("a", {"rel": "noopener follow"})
                    if x.text.endswith("Follower") or x.text.endswith("Followers")
                }
            )
            if num_followers_base == []:
                num_followers = 0
            else:
                num_followers_raw = (
                    num_followers_base[0].replace(" Followers", "").replace(" Follower", "")
                )
                if num_followers_raw.endswith("K"):
                    num_followers = int(float(num_followers_raw[:-1]) * 1000)
                else:
                    num_followers = int(num_followers_raw)

            div_data = [x.text for x in soup.find_all("div")]
            if num_followers == 1:
                base = div_data[0][div_data[0].rfind("Follower") + 8 :]
            else:
                base = div_data[0][div_data[0].rfind("Follower") + 9 :]
            short_bio = base[: base.find("Follow")]
            if short_bio.startswith("appSign upSign InWriteSign"):
                short_bio = ""  # Accounting for authors with no biography

            return {
                "extraction_id": get_extraction_id(),
                "extracted_at": get_extracted_at(),
                "extracted_at_epoch": get_extracted_at_epoch(),
                "author_name": author_name,
                "author_url": author_url,
                "num_followers": num_followers,
                "short_bio": short_bio,
            }

        data = [
            x
            for x in get_json_content(domain="medium_blogs")
            if x["extraction_id"] == extraction_id
        ]

        author_urls = list(set({x["author_url"] for x in data}))
        logger.info(f"Found {len(author_urls)} distinct authors.")

        pool = ThreadPool(8)
        author_data = pool.map(
            lambda author_url: medium_scrape_authors(str(author_url)),
            author_urls[0:],
        )

        return author_data

    def scrape_blogs(self, tag: str) -> List[Dict[str, object]]:
        url = f"https://medium.com/tag/{tag}/archive"
        logger.info(f"Scraping blogs from {url=}...")

        headers = {
            "content-type": "application/json",
            "graphql-operation": "TagArchiveFeedQuery",
            "referer": f"https://medium.com/tag/{tag}/archive",
        }

        # Posts come back newest first, so page until a page reaches past the start of the
        # lookback window (midnight, matching the date-based filter in the weekly email).
        cutoff = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=self.lookback_days
        )
        raw_data: List[Dict[str, Any]] = []
        after = None
        for page_number in range(1, self.max_pages + 1):
            feed = self.fetch_tag_feed_page(headers=headers, tag=tag, after=after)
            raw_data.extend(feed["edges"])
            if not feed["edges"] or not feed["pageInfo"]["hasNextPage"]:
                break
            oldest_published_at = datetime.fromtimestamp(
                feed["edges"][-1]["node"]["firstPublishedAt"] / 1000
            )
            if oldest_published_at < cutoff:
                break
            after = feed["pageInfo"]["endCursor"]
            time.sleep(1)  # To avoid rate limit detection
        else:
            logger.warning(
                f"Hit the {self.max_pages}-page cap for tag '{tag}' before reaching {cutoff}, "
                "older blogs in the lookback window are missing."
            )
        logger.info(f"Retrieved {len(raw_data)} blogs across {page_number} page(s)...")

        base_data = {
            "extraction_id": get_extraction_id(),
            "extracted_at": get_extracted_at(),
            "extracted_at_epoch": get_extracted_at_epoch(),
            "extraction_url": url,
            "tag": tag,
        }

        blog_data = []
        for i in raw_data:
            info = {
                "author_name": i["node"]["creator"]["name"],
                "author_url": "https://medium.com/@" + i["node"]["creator"]["username"],
                "published_at": datetime.fromtimestamp(i["node"]["firstPublishedAt"] / 1000),
                "reading_time_minutes": i["node"]["readingTime"],
                "story_url": i["node"]["mediumUrl"],
                "subtitle": i["node"]["extendedPreviewContent"]["subtitle"],
                "title": i["node"]["title"],
            }

            sorted_data = dict(sorted(info.items(), key=lambda x: (x[0])))
            blog_data.append({**sorted_data, **base_data})

        return blog_data

    def fetch_tag_feed_page(
        self, headers: Dict[str, str], tag: str, after: Optional[str]
    ) -> Dict[str, Any]:
        variables: Dict[str, Any] = {
            "tagSlug": tag,
            "timeRange": {
                "kind": "ALL_TIME",
            },
            "sortOrder": "NEWEST",
            "first": self.page_size,
        }
        if after:
            variables["after"] = after

        json_data = [
            {
                "operationName": "TagArchiveFeedQuery",
                "variables": variables,
                "query": "query TagArchiveFeedQuery($tagSlug: String!, $timeRange: TagPostsTimeRange!, $sortOrder: TagPostsSortOrder!, $first: Int!, $after: String) {\n  tagFromSlug(tagSlug: $tagSlug) {\n    id\n    sortedFeed: posts(\n      timeRange: $timeRange\n      sortOrder: $sortOrder\n      first: $first\n      after: $after\n    ) {\n      ...TagPosts_tagPostConnection\n      __typename\n    }\n    mostReadFeed: posts(timeRange: $timeRange, sortOrder: MOST_READ, first: $first) {\n      ...TagPosts_tagPostConnection\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment TagPosts_tagPostConnection on TagPostConnection {\n  edges {\n    cursor\n    node {\n      id\n      ...StreamPostPreview_post\n      __typename\n    }\n    __typename\n  }\n  pageInfo {\n    hasNextPage\n    endCursor\n    __typename\n  }\n  __typename\n}\n\nfragment StreamPostPreview_post on Post {\n  id\n  ...StreamPostPreviewContent_post\n  ...PostPreviewContainer_post\n  __typename\n}\n\nfragment StreamPostPreviewContent_post on Post {\n  id\n  title\n  previewImage {\n    id\n    __typename\n  }\n  extendedPreviewContent {\n    subtitle\n    __typename\n  }\n  ...StreamPostPreviewImage_post\n  ...PostPreviewFooter_post\n  ...PostPreviewByLine_post\n  ...PostPreviewInformation_post\n  __typename\n}\n\nfragment StreamPostPreviewImage_post on Post {\n  title\n  previewImage {\n    ...StreamPostPreviewImage_imageMetadata\n    __typename\n    id\n  }\n  __typename\n  id\n}\n\nfragment StreamPostPreviewImage_imageMetadata on ImageMetadata {\n  id\n  focusPercentX\n  focusPercentY\n  alt\n  __typename\n}\n\nfragment PostPreviewFooter_post on Post {\n  ...PostPreviewFooterSocial_post\n  ...PostPreviewFooterMenu_post\n  __typename\n  id\n}\n\nfragment PostPreviewFooterSocial_post on Post {\n  id\n  ...MultiVote_post\n  allowResponses\n  isLimitedState\n  postResponses {\n    count\n    __typename\n  }\n  __typename\n}\n\nfragment MultiVote_post on Post {\n  id\n  creator {\n    id\n    ...SusiClickable_user\n    __typename\n  }\n  isPublished\n  ...SusiClickable_post\n  collection {\n    id\n    slug\n    __typename\n  }\n  isLimitedState\n  ...MultiVoteCount_post\n  __typename\n}\n\nfragment SusiClickable_user on User {\n  ...SusiContainer_user\n  __typename\n  id\n}\n\nfragment SusiContainer_user on User {\n  ...SignInOptions_user\n  ...SignUpOptions_user\n  __typename\n  id\n}\n\nfragment SignInOptions_user on User {\n  id\n  name\n  __typename\n}\n\nfragment SignUpOptions_user on User {\n  id\n  name\n  __typename\n}\n\nfragment SusiClickable_post on Post {\n  id\n  mediumUrl\n  ...SusiContainer_post\n  __typename\n}\n\nfragment SusiContainer_post on Post {\n  id\n  __typename\n}\n\nfragment MultiVoteCount_post on Post {\n  id\n  __typename\n}\n\nfragment PostPreviewFooterMenu_post on Post {\n  creator {\n    __typename\n    id\n  }\n  collection {\n    __typename\n    id\n  }\n  ...BookmarkButton_post\n  ...ExpandablePostCardOverflowButton_post\n  __typename\n  id\n}\n\nfragment BookmarkButton_post on Post {\n  visibility\n  ...SusiClickable_post\n  ...AddToCatalogBookmarkButton_post\n  __typename\n  id\n}\n\nfragment AddToCatalogBookmarkButton_post on Post {\n  ...AddToCatalogBase_post\n  __typename\n  id\n}\n\nfragment AddToCatalogBase_post on Post {\n  id\n  isPublished\n  __typename\n}\n\nfragment ExpandablePostCardOverflowButton_post on Post {\n  creator {\n    id\n    __typename\n  }\n  ...ExpandablePostCardReaderButton_post\n  __typename\n  id\n}\n\nfragment ExpandablePostCardReaderButton_post on Post {\n  id\n  collection {\n    id\n    __typename\n  }\n  creator {\n    id\n    __typename\n  }\n  clapCount\n  ...ClapMutation_post\n  __typename\n}\n\nfragment ClapMutation_post on Post {\n  __typename\n  id\n  clapCount\n  ...MultiVoteCount_post\n}\n\nfragment PostPreviewByLine_post on Post {\n  id\n  creator {\n    ...PostPreviewByLine_user\n    __typename\n    id\n  }\n  collection {\n    ...PostPreviewByLine_collection\n    __typename\n    id\n  }\n  ...CardByline_post\n  __typename\n}\n\nfragment PostPreviewByLine_user on User {\n  id\n  __typename\n  ...CardByline_user\n  ...ExpandablePostByline_user\n}\n\nfragment CardByline_user on User {\n  __typename\n  id\n  name\n  username\n  mediumMemberAt\n  socialStats {\n    followerCount\n    __typename\n  }\n  ...useIsVerifiedBookAuthor_user\n  ...userUrl_user\n  ...UserMentionTooltip_user\n}\n\nfragment useIsVerifiedBookAuthor_user on User {\n  verifications {\n    isBookAuthor\n    __typename\n  }\n  __typename\n  id\n}\n\nfragment userUrl_user on User {\n  __typename\n  id\n  customDomainState {\n    live {\n      domain\n      __typename\n    }\n    __typename\n  }\n  hasSubdomain\n  username\n}\n\nfragment UserMentionTooltip_user on User {\n  id\n  name\n  username\n  bio\n  imageId\n  mediumMemberAt\n  membership {\n    tier\n    __typename\n    id\n  }\n  ...UserAvatar_user\n  ...UserFollowButton_user\n  ...useIsVerifiedBookAuthor_user\n  __typename\n}\n\nfragment UserAvatar_user on User {\n  __typename\n  id\n  imageId\n  mediumMemberAt\n  membership {\n    tier\n    __typename\n    id\n  }\n  name\n  username\n  ...userUrl_user\n}\n\nfragment UserFollowButton_user on User {\n  ...UserFollowButtonSignedIn_user\n  ...UserFollowButtonSignedOut_user\n  __typename\n  id\n}\n\nfragment UserFollowButtonSignedIn_user on User {\n  id\n  name\n  __typename\n}\n\nfragment UserFollowButtonSignedOut_user on User {\n  id\n  ...SusiClickable_user\n  __typename\n}\n\nfragment ExpandablePostByline_user on User {\n  __typename\n  id\n  name\n  imageId\n  ...userUrl_user\n  ...useIsVerifiedBookAuthor_user\n}\n\nfragment PostPreviewByLine_collection on Collection {\n  id\n  __typename\n  ...CardByline_collection\n  ...CollectionLinkWithPopover_collection\n}\n\nfragment CardByline_collection on Collection {\n  name\n  ...collectionUrl_collection\n  __typename\n  id\n}\n\nfragment collectionUrl_collection on Collection {\n  id\n  domain\n  slug\n  __typename\n}\n\nfragment CollectionLinkWithPopover_collection on Collection {\n  ...collectionUrl_collection\n  ...CollectionTooltip_collection\n  __typename\n  id\n}\n\nfragment CollectionTooltip_collection on Collection {\n  id\n  name\n  description\n  subscriberCount\n  ...CollectionAvatar_collection\n  ...CollectionFollowButton_collection\n  __typename\n}\n\nfragment CollectionAvatar_collection on Collection {\n  name\n  avatar {\n    id\n    __typename\n  }\n  ...collectionUrl_collection\n  __typename\n  id\n}\n\nfragment CollectionFollowButton_collection on Collection {\n  __typename\n  id\n  name\n  slug\n  ...collectionUrl_collection\n  ...SusiClickable_collection\n}\n\nfragment SusiClickable_collection on Collection {\n  ...SusiContainer_collection\n  __typename\n  id\n}\n\nfragment SusiContainer_collection on Collection {\n  name\n  ...SignInOptions_collection\n  ...SignUpOptions_collection\n  __typename\n  id\n}\n\nfragment SignInOptions_collection on Collection {\n  id\n  name\n  __typename\n}\n\nfragment SignUpOptions_collection on Collection {\n  id\n  name\n  __typename\n}\n\nfragment CardByline_post on Post {\n  ...DraftStatus_post\n  ...Star_post\n  ...shouldShowPublishedInStatus_post\n  __typename\n  id\n}\n\nfragment DraftStatus_post on Post {\n  id\n  pendingCollection {\n    id\n    creator {\n      id\n      __typename\n    }\n    ...BoldCollectionName_collection\n    __typename\n  }\n  statusForCollection\n  creator {\n    id\n    __typename\n  }\n  isPublished\n  __typename\n}\n\nfragment BoldCollectionName_collection on Collection {\n  id\n  name\n  __typename\n}\n\nfragment Star_post on Post {\n  id\n  creator {\n    id\n    __typename\n  }\n  __typename\n}\n\nfragment shouldShowPublishedInStatus_post on Post {\n  statusForCollection\n  isPublished\n  __typename\n  id\n}\n\nfragment PostPreviewInformation_post on Post {\n  pinnedAt\n  latestPublishedAt\n  firstPublishedAt\n  readingTime\n  isLocked\n  ...Star_post\n  __typename\n  id\n}\n\nfragment PostPreviewContainer_post on Post {\n  id\n  extendedPreviewContent {\n    isFullContent\n    __typename\n  }\n  visibility\n  pinnedAt\n  ...PostScrollTracker_post\n  ...usePostUrl_post\n  __typename\n}\n\nfragment PostScrollTracker_post on Post {\n  id\n  collection {\n    id\n    __typename\n  }\n  sequence {\n    sequenceId\n    __typename\n  }\n  __typename\n}\n\nfragment usePostUrl_post on Post {\n  id\n  creator {\n    ...userUrl_user\n    __typename\n    id\n  }\n  collection {\n    id\n    domain\n    slug\n    __typename\n  }\n  isSeries\n  mediumUrl\n  sequence {\n    slug\n    __typename\n  }\n  uniqueSlug\n  __typename\n}\n",
            },
        ]

        response = medium_post_graphql_request(headers=headers, json=json_data)
        return response[0]["data"]["tagFromSlug"]["sortedFeed"]  # type: ignore[index, no-any-return]
