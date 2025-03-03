class TextTransform(object):
    """
    Defines and interface for operations that accept content text as an
    input and return the text after some transformation has been performed
    """

    def transform_content(self, input_text: str) -> str:
        """
        Transform input_text and return the result
        """
        raise NotImplementedError
