import os

from utils.utils import set_logging_options

from .bi_plotly import BuildPlotlyHTMLFile
from .email_weekly_medium_blogs import SendMediumBlogsEmail


def main():
    set_logging_options()

    # MEDIUM_BLOG_LOOKBACK_WINDOW is "" when action is triggered from main branch
    default_lookback = 7
    try:
        lookback_days = int(os.getenv("MEDIUM_BLOG_LOOKBACK_WINDOW", default_lookback))
    except:
        lookback_days = default_lookback

    BuildPlotlyHTMLFile().run()
    SendMediumBlogsEmail(lookback_days=lookback_days).run()


if __name__ == "__main__":
    main()