import re
import html
from timpani.conductor.transforms.text_transform import TextTransform
from timpani.util.exceptions import UnuseableContentException


class TwitterTextTransforms(TextTransform):
    """
    Implements transform patterns for removing text content
    commonly found in twitter content that is likely to inflate similarity:

    URLS
    Retweet shorthand signifiers
    Mentions
    Unescaped HTML content
    """

    RE_URL = re.compile(
        r"((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)", re.I
    )

    RE_MENTION = re.compile(r"@[0-9a-z_]+", re.I)
    # TODO: should we also remove 'retweet'?
    # and also 'via @xxxxx'

    RE_RT = re.compile(r"^RT[: \-]?", re.I)

    # https://stackoverflow.com/questions/33404752/removing-emojis-from-a-string-in-python
    # plus links from https://en.wikipedia.org/wiki/Emoticons_(Unicode_block)
    # TODO: this is not very twitter-specific, so probably should move to more general cleaning for embeddings
    RE_EMOJI = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001F300-\U0001F5FF"  # Miscellaneous Symbols and Pictographs
        "\U00002700-\U000027BF"  # Dingbats
        "\U00002600-\U000026FF"  # Miscellaneous Symbols
        "\U0001F680-\U0001F6FF"  # Transport and Map Symbols
        "]{2,}",
        flags=re.UNICODE,
    )

    def transform_content(self, input_text: str) -> str:
        """
        Transform input_text and return the result, chaining multiple transforms in sequence
        * unescape html text
        * remove mentions
        * remove urls
        * remove retweet signifiers
        """
        # convert any html entities
        output_text = html.unescape(input_text)
        # replace any matching text with a space
        output_text = re.sub(self.RE_MENTION, " ", output_text)

        # we remove all URLs, mostly they have been replaced by shortner https://t.co/
        # so do not have useful content for similarity
        output_text = re.sub(self.RE_URL, " ", output_text)

        # common twitter shorthand (like RT for retweet) is not helpful for similarity
        output_text = re.sub(self.RE_RT, " ", output_text)

        # clusters are gettting formed with lots of similar emoji strings (but text otherwise not similar)
        # match two or more emoji (but leave one alone)
        output_text = re.sub(self.RE_EMOJI, " ", output_text)

        # remove any leading or trailing whitespace
        output_text = output_text.strip()

        # if the result is an empty string, raise an error to put item into failed state
        # and not try to process it further
        if output_text == "":
            raise UnuseableContentException(
                "Transformation of twitter text content resulted in an empty string"
            )

        return output_text
