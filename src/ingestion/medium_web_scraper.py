import time
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Dict, List

from bs4 import BeautifulSoup
from retry import retry

from utils.logger import logger
from utils.utils import (
    create_requests_session,
    get_extracted_at,
    get_extracted_at_epoch,
    get_extraction_id,
    get_json_content,
    save_to_landing_zone,
)


class MediumWebScraper:
    def __init__(self, tags: List[str]):
        self.medium_blog_limit = 100  # Hard-coding for simplicity, previously was selectable
        self.tags = tags

    def run(self) -> None:
        for tag in self.tags:
            logger.info(f"Scraping {self.medium_blog_limit} blogs with tag '{tag}'...")

            scraped_data = self.scrape_blogs(tag=tag)

            save_to_landing_zone(
                data=[dict(t) for t in {tuple(d.items()) for d in scraped_data}],
                file_name=f"domain=medium_blogs/tag={tag}/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
            )

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

        json_data = [
            {
                "operationName": "TagArchiveFeedQuery",
                "variables": {
                    "tagSlug": tag,
                    "timeRange": {
                        "kind": "ALL_TIME",
                    },
                    "sortOrder": "NEWEST",
                    "first": self.medium_blog_limit,
                },
                "query": "query TagArchiveFeedQuery($tagSlug: String!, $timeRange: TagPostsTimeRange!, $sortOrder: TagPostsSortOrder!, $first: Int!, $after: String) {\n  tagFromSlug(tagSlug: $tagSlug) {\n    id\n    sortedFeed: posts(\n      timeRange: $timeRange\n      sortOrder: $sortOrder\n      first: $first\n      after: $after\n    ) {\n      ...TagPosts_tagPostConnection\n      __typename\n    }\n    mostReadFeed: posts(timeRange: $timeRange, sortOrder: MOST_READ, first: $first) {\n      ...TagPosts_tagPostConnection\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment TagPosts_tagPostConnection on TagPostConnection {\n  edges {\n    cursor\n    node {\n      id\n      ...StreamPostPreview_post\n      __typename\n    }\n    __typename\n  }\n  pageInfo {\n    hasNextPage\n    endCursor\n    __typename\n  }\n  __typename\n}\n\nfragment StreamPostPreview_post on Post {\n  id\n  ...StreamPostPreviewContent_post\n  ...PostPreviewContainer_post\n  __typename\n}\n\nfragment StreamPostPreviewContent_post on Post {\n  id\n  title\n  previewImage {\n    id\n    __typename\n  }\n  extendedPreviewContent {\n    subtitle\n    __typename\n  }\n  ...StreamPostPreviewImage_post\n  ...PostPreviewFooter_post\n  ...PostPreviewByLine_post\n  ...PostPreviewInformation_post\n  __typename\n}\n\nfragment StreamPostPreviewImage_post on Post {\n  title\n  previewImage {\n    ...StreamPostPreviewImage_imageMetadata\n    __typename\n    id\n  }\n  __typename\n  id\n}\n\nfragment StreamPostPreviewImage_imageMetadata on ImageMetadata {\n  id\n  focusPercentX\n  focusPercentY\n  alt\n  __typename\n}\n\nfragment PostPreviewFooter_post on Post {\n  ...PostPreviewFooterSocial_post\n  ...PostPreviewFooterMenu_post\n  __typename\n  id\n}\n\nfragment PostPreviewFooterSocial_post on Post {\n  id\n  ...MultiVote_post\n  allowResponses\n  isLimitedState\n  postResponses {\n    count\n    __typename\n  }\n  __typename\n}\n\nfragment MultiVote_post on Post {\n  id\n  creator {\n    id\n    ...SusiClickable_user\n    __typename\n  }\n  isPublished\n  ...SusiClickable_post\n  collection {\n    id\n    slug\n    __typename\n  }\n  isLimitedState\n  ...MultiVoteCount_post\n  __typename\n}\n\nfragment SusiClickable_user on User {\n  ...SusiContainer_user\n  __typename\n  id\n}\n\nfragment SusiContainer_user on User {\n  ...SignInOptions_user\n  ...SignUpOptions_user\n  __typename\n  id\n}\n\nfragment SignInOptions_user on User {\n  id\n  name\n  __typename\n}\n\nfragment SignUpOptions_user on User {\n  id\n  name\n  __typename\n}\n\nfragment SusiClickable_post on Post {\n  id\n  mediumUrl\n  ...SusiContainer_post\n  __typename\n}\n\nfragment SusiContainer_post on Post {\n  id\n  __typename\n}\n\nfragment MultiVoteCount_post on Post {\n  id\n  __typename\n}\n\nfragment PostPreviewFooterMenu_post on Post {\n  creator {\n    __typename\n    id\n  }\n  collection {\n    __typename\n    id\n  }\n  ...BookmarkButton_post\n  ...ExpandablePostCardOverflowButton_post\n  __typename\n  id\n}\n\nfragment BookmarkButton_post on Post {\n  visibility\n  ...SusiClickable_post\n  ...AddToCatalogBookmarkButton_post\n  __typename\n  id\n}\n\nfragment AddToCatalogBookmarkButton_post on Post {\n  ...AddToCatalogBase_post\n  __typename\n  id\n}\n\nfragment AddToCatalogBase_post on Post {\n  id\n  isPublished\n  __typename\n}\n\nfragment ExpandablePostCardOverflowButton_post on Post {\n  creator {\n    id\n    __typename\n  }\n  ...ExpandablePostCardReaderButton_post\n  __typename\n  id\n}\n\nfragment ExpandablePostCardReaderButton_post on Post {\n  id\n  collection {\n    id\n    __typename\n  }\n  creator {\n    id\n    __typename\n  }\n  clapCount\n  ...ClapMutation_post\n  __typename\n}\n\nfragment ClapMutation_post on Post {\n  __typename\n  id\n  clapCount\n  ...MultiVoteCount_post\n}\n\nfragment PostPreviewByLine_post on Post {\n  id\n  creator {\n    ...PostPreviewByLine_user\n    __typename\n    id\n  }\n  collection {\n    ...PostPreviewByLine_collection\n    __typename\n    id\n  }\n  ...CardByline_post\n  __typename\n}\n\nfragment PostPreviewByLine_user on User {\n  id\n  __typename\n  ...CardByline_user\n  ...ExpandablePostByline_user\n}\n\nfragment CardByline_user on User {\n  __typename\n  id\n  name\n  username\n  mediumMemberAt\n  socialStats {\n    followerCount\n    __typename\n  }\n  ...useIsVerifiedBookAuthor_user\n  ...userUrl_user\n  ...UserMentionTooltip_user\n}\n\nfragment useIsVerifiedBookAuthor_user on User {\n  verifications {\n    isBookAuthor\n    __typename\n  }\n  __typename\n  id\n}\n\nfragment userUrl_user on User {\n  __typename\n  id\n  customDomainState {\n    live {\n      domain\n      __typename\n    }\n    __typename\n  }\n  hasSubdomain\n  username\n}\n\nfragment UserMentionTooltip_user on User {\n  id\n  name\n  username\n  bio\n  imageId\n  mediumMemberAt\n  membership {\n    tier\n    __typename\n    id\n  }\n  ...UserAvatar_user\n  ...UserFollowButton_user\n  ...useIsVerifiedBookAuthor_user\n  __typename\n}\n\nfragment UserAvatar_user on User {\n  __typename\n  id\n  imageId\n  mediumMemberAt\n  membership {\n    tier\n    __typename\n    id\n  }\n  name\n  username\n  ...userUrl_user\n}\n\nfragment UserFollowButton_user on User {\n  ...UserFollowButtonSignedIn_user\n  ...UserFollowButtonSignedOut_user\n  __typename\n  id\n}\n\nfragment UserFollowButtonSignedIn_user on User {\n  id\n  name\n  __typename\n}\n\nfragment UserFollowButtonSignedOut_user on User {\n  id\n  ...SusiClickable_user\n  __typename\n}\n\nfragment ExpandablePostByline_user on User {\n  __typename\n  id\n  name\n  imageId\n  ...userUrl_user\n  ...useIsVerifiedBookAuthor_user\n}\n\nfragment PostPreviewByLine_collection on Collection {\n  id\n  __typename\n  ...CardByline_collection\n  ...CollectionLinkWithPopover_collection\n}\n\nfragment CardByline_collection on Collection {\n  name\n  ...collectionUrl_collection\n  __typename\n  id\n}\n\nfragment collectionUrl_collection on Collection {\n  id\n  domain\n  slug\n  __typename\n}\n\nfragment CollectionLinkWithPopover_collection on Collection {\n  ...collectionUrl_collection\n  ...CollectionTooltip_collection\n  __typename\n  id\n}\n\nfragment CollectionTooltip_collection on Collection {\n  id\n  name\n  description\n  subscriberCount\n  ...CollectionAvatar_collection\n  ...CollectionFollowButton_collection\n  __typename\n}\n\nfragment CollectionAvatar_collection on Collection {\n  name\n  avatar {\n    id\n    __typename\n  }\n  ...collectionUrl_collection\n  __typename\n  id\n}\n\nfragment CollectionFollowButton_collection on Collection {\n  __typename\n  id\n  name\n  slug\n  ...collectionUrl_collection\n  ...SusiClickable_collection\n}\n\nfragment SusiClickable_collection on Collection {\n  ...SusiContainer_collection\n  __typename\n  id\n}\n\nfragment SusiContainer_collection on Collection {\n  name\n  ...SignInOptions_collection\n  ...SignUpOptions_collection\n  __typename\n  id\n}\n\nfragment SignInOptions_collection on Collection {\n  id\n  name\n  __typename\n}\n\nfragment SignUpOptions_collection on Collection {\n  id\n  name\n  __typename\n}\n\nfragment CardByline_post on Post {\n  ...DraftStatus_post\n  ...Star_post\n  ...shouldShowPublishedInStatus_post\n  __typename\n  id\n}\n\nfragment DraftStatus_post on Post {\n  id\n  pendingCollection {\n    id\n    creator {\n      id\n      __typename\n    }\n    ...BoldCollectionName_collection\n    __typename\n  }\n  statusForCollection\n  creator {\n    id\n    __typename\n  }\n  isPublished\n  __typename\n}\n\nfragment BoldCollectionName_collection on Collection {\n  id\n  name\n  __typename\n}\n\nfragment Star_post on Post {\n  id\n  creator {\n    id\n    __typename\n  }\n  __typename\n}\n\nfragment shouldShowPublishedInStatus_post on Post {\n  statusForCollection\n  isPublished\n  __typename\n  id\n}\n\nfragment PostPreviewInformation_post on Post {\n  pinnedAt\n  latestPublishedAt\n  firstPublishedAt\n  readingTime\n  isLocked\n  ...Star_post\n  __typename\n  id\n}\n\nfragment PostPreviewContainer_post on Post {\n  id\n  extendedPreviewContent {\n    isFullContent\n    __typename\n  }\n  visibility\n  pinnedAt\n  ...PostScrollTracker_post\n  ...usePostUrl_post\n  __typename\n}\n\nfragment PostScrollTracker_post on Post {\n  id\n  collection {\n    id\n    __typename\n  }\n  sequence {\n    sequenceId\n    __typename\n  }\n  __typename\n}\n\nfragment usePostUrl_post on Post {\n  id\n  creator {\n    ...userUrl_user\n    __typename\n    id\n  }\n  collection {\n    id\n    domain\n    slug\n    __typename\n  }\n  isSeries\n  mediumUrl\n  sequence {\n    slug\n    __typename\n  }\n  uniqueSlug\n  __typename\n}\n",
            },
        ]

        response = create_requests_session().post(
            "https://medium.com/_/graphql", headers=headers, json=json_data
        )
        logger.info(
            f'Retrieved {len(response.json()[0]["data"]["tagFromSlug"]["sortedFeed"]["edges"])} blogs...'
        )

        base_data = {
            "extraction_id": get_extraction_id(),
            "extracted_at": get_extracted_at(),
            "extracted_at_epoch": get_extracted_at_epoch(),
            "extraction_url": url,
            "tag": tag,
        }

        blog_data = []
        raw_data = response.json()[0]["data"]["tagFromSlug"]["sortedFeed"]["edges"]
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
