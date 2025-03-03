import streamlit as st
import streamlit.components.v1 as components
import requests


class TikTokEmbed(object):
    """
    Display the embedded tweet object for a TikTok url
    From https://developers.tiktok.com/doc/embed-videos/
    """

    @st.cache_data(ttl=3600)
    def fetch_tiktok_embed_html(url):
        # Use TikTok's oEmbed API to fetch html for the video embed
        # https://developers.tiktok.com/doc/embed-videos/
        api = f"https://www.tiktok.com/oembed?url={url}"
        response = requests.get(api)
        html = f"fetching {url}"
        if response.ok:
            html = response.json()["html"]
        else:
            html = f"{url}<br>(Error requesting embed:{response})"
        return html

    def __init__(self, url, embed_str=False):
        if not embed_str:
            self.text = TikTokEmbed.fetch_tiktok_embed_html(url)
        else:
            self.text = url

    def _repr_html_():
        """
        This seems to be needed to work around a bug
        """
        return ""

    def component(self):
        return components.html(self.text, height=600, scrolling=True)
