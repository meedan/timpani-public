import streamlit as st
import streamlit.components.v1 as components


class FacebookEmbed(object):
    """
    Display the embedded facebook widget for a public post url
    https://developers.facebook.com/docs/plugins/embedded-posts/
    """

    @st.cache_data(ttl=3600)
    def fetch_facebook_embed_html(fb_url: str):

        # load javascript for facebook embedding
        html = f"""
        <div id="fb-root"></div>
        <script async defer src="https://connect.facebook.net/en_US/sdk.js#xfbml=1&version=v3.2"></script>
        <div class="fb-post" data-width="340" data-href="{fb_url}"></div>
        """
        return html

    def __init__(self, fb_url, embed_str=False):
        if not embed_str:
            self.text = FacebookEmbed.fetch_facebook_embed_html(fb_url)
        else:
            self.text = f"""
            <a href='{fb_url}' target="_blank" rel="noopener noreferrer">view on Facebook</a>
            """

    def _repr_html_():
        """
        This seems to be needed to work around a bug
        """
        return ""

    def component(self):
        return components.html(f"<body>{self.text}</body>", height=600, scrolling=True)
