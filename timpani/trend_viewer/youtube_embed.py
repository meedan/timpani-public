import streamlit as st
import streamlit.components.v1 as components


class YouTubeEmbed(object):
    """
    Display the embedded telegram widge for a public telegram url
    https://core.telegram.org/widgets/post

    Based on https://discuss.streamlit.io/t/dispalying-a-tweet/16061
    """

    @st.cache_data(ttl=3600)
    def fetch_youtube_embed_html(yt_url: str):
        # https://support.google.com/youtube/answer/171780?hl=en
        # split off the t.me part to get the post id.
        # so "https://www.youtube.com/watch?v=y7YaKNmmPnI"
        # becomes y7YaKNmmPnI
        post_id = yt_url.replace("https://www.youtube.com/watch?v=", "")
        html = f"""
        <iframe width="560" height="315" src="https://www.youtube.com/embed/{post_id}"
        frameborder="0" allow="encrypted-media;web-share"></iframe>
        <br><a href='{yt_url}' target="_blank" rel="noopener noreferrer">view on YouTube</a>
        """
        return html

    def __init__(self, yt_url, embed_str=False):
        if not embed_str:
            self.text = YouTubeEmbed.fetch_youtube_embed_html(yt_url)
        else:
            self.text = yt_url

    def _repr_html_():
        """
        This seems to be needed to work around a bug
        """
        return ""

    def component(self):
        return components.html(self.text, height=600, scrolling=True)
