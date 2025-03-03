import streamlit as st
import streamlit.components.v1 as components


class TelegramEmbed(object):
    """
    Display the embedded telegram widget for a public telegram url
    https://core.telegram.org/widgets/post

    Based on https://discuss.streamlit.io/t/dispalying-a-tweet/16061
    """

    @st.cache_data(ttl=3600)
    def fetch_telegram_embed_html(t_url: str):
        # https://core.telegram.org/widgets/post
        # split off the t.me part to get the post id.
        # so https://t.me/EarthJusticeLeague_DataLibrary/11687
        # becomes EarthJusticeLeague_DataLibrary/11687
        post_id = t_url.replace("https://t.me/", "")
        html = f"""
        <script async src="https://telegram.org/js/telegram-widget.js?22"
        data-telegram-post="{post_id}" data-width="100%"></script>
        """
        return html

    def __init__(self, t_url, embed_str=False):
        if not embed_str:
            self.text = TelegramEmbed.fetch_telegram_embed_html(t_url)
        else:
            self.text = f"""
            <a href='{t_url}' target="_blank" rel="noopener noreferrer">view on Telegram</a>
            """

    def _repr_html_():
        """
        This seems to be needed to work around a bug
        """
        return ""

    def component(self):
        return components.html(f"<body>{self.text}</body>", height=600, scrolling=True)
