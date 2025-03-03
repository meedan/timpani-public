import streamlit as st
import streamlit.components.v1 as components
import requests


class TweetEmbed(object):
    """
    Display the embedded tweet object for a twitter url
    From https://discuss.streamlit.io/t/dispalying-a-tweet/16061
    """

    @st.cache_data(ttl=3600)
    def fetch_tweet_embed_html(tweet_url):
        # Use Twitter's oEmbed API
        # https://dev.twitter.com/web/embedded-tweets
        api = f"https://publish.twitter.com/oembed?url={tweet_url}"
        response = requests.get(api)
        html = f"fetching {tweet_url}"
        if response.ok:
            html = response.json()["html"]
        else:
            html = f"{tweet_url}<br>(Error requesting embed:{response})"
        return html

    def __init__(self, tweet_url, embed_str=False):
        if not embed_str:
            self.text = TweetEmbed.fetch_tweet_embed_html(tweet_url)
        else:
            self.text = tweet_url

    def _repr_html_():
        """
        This seems to be needed to work around a bug
        """
        return ""

    def component(self):
        return components.html(self.text, height=600, scrolling=True)
