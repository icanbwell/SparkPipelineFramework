import traceback


class FriendlySparkException(Exception):
    # noinspection PyUnusedLocal
    def __init__(self, *args, **kwargs):
        try:
            # Summary is a boolean argument
            # If True, it prints the exception summary
            # This way, we can avoid printing the summary all
            # the way along the exception "bubbling up"
            stage_name = kwargs['stage_name']
            error_text = (stage_name or '') + ": " + FriendlySparkException.exception_summary()
            Exception.__init__(self, error_text)
        except KeyError:
            pass

    # noinspection SpellCheckingInspection
    @staticmethod
    def errortext(text):
        # Makes exception summary both BOLD and RED (FAIL)
        return text

    @staticmethod
    def exception_summary() -> str:
        # Gets the error stack
        msg = traceback.format_exc()
        try:
            # Builds the "frame" around the text
            # Gets the information about the error and makes it BOLD and RED
            info = list(filter(lambda t: len(t) and t[0] != '\t', msg.split('\n')[::-1]))
            error = FriendlySparkException.errortext('Error\t: {}'.format(info[0]))
            # Figure out where the error happened - location (file/notebook), line and function
            idx = [t.strip()[:4] for t in info].index('File')
            where = [v.strip() for v in info[idx].strip().split(',')]
            location, line, func = where[0][5:], where[1][5:], where[2][3:]
            # If it is a pyspark error, just go with it
            if 'pyspark' in error:
                new_msg = '\n{}'.format(error)
            # Otherwise, build the summary
            else:
                new_msg = '\nLocation: {}\nLine\t: {}\nFunction: {}\n{}'.format(location, line, func, error)
            return new_msg
        except Exception as e:
            # If we managed to raise an exception while trying to format the original exception...
            # Oh, well...
            return 'This is awkward... \n{}'.format(str(e))
