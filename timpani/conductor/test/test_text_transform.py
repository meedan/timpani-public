import unittest
from timpani.conductor.transforms.twitter import TwitterTextTransforms
from timpani.util.exceptions import UnuseableContentException


class TestTextTransforms(unittest.TestCase):
    """
    Test functions of text transformation classes
    """

    def test_twitter_text_transforms(self):
        """
        Validate thats mentions, retweets, etc are removed
        """

        transform = TwitterTextTransforms()

        # key is expected input, value is expected output
        test_examples = {
            "田林派出所暴行录 https://t.co/STw5qMixkM": "田林派出所暴行录",
            "https://t.co/2xbUQpbBqe via @YouTube Lủ ác hèn gây án phá nát": "via   Lủ ác hèn gây án phá nát",
            "#RamdevOnIndiaTV | बदनदर्द..स्ट्रेस..पेट में ऐंठन..कैसे": "#RamdevOnIndiaTV | बदनदर्द..स्ट्रेस..पेट में ऐंठन..कैसे",
            "Would @charliekirk11 @TPUSA invite natural constituents": "Would     invite natural constituents",
            "the characters < and >, are encoded as &lt; and &gt;": "the characters < and >, are encoded as < and >",
            "反共辱华乐此不彼 🙏👏🤝👍😂😅🤣✋": "反共辱华乐此不彼",  # long emoji strings seem to confuse models
            "limited or suspended mRNA 💉s": "limited or suspended mRNA 💉s",  # don't clobber single emoji
        }

        for example in test_examples:
            transformed = transform.transform_content(example)
            assert (
                transformed == test_examples[example]
            ), f"transformed value '{transformed}' did not match expected '{test_examples[example]}'"

        # confirm that clobbering all the text will throw error
        with self.assertRaises(UnuseableContentException):
            transformed = transform.transform_content(
                "@elonmusk https://t.co/96GnHYtqQN"
            )
            assert transformed == ""
