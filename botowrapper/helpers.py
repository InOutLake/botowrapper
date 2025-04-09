from functools import wraps


class BucketIsNotSelectedException(Exception):
    """Use whenever operation on bucket performed before bucket was selected"""

    def __init__(self, message: str | None):
        if not message:
            message = "Bucket is not selected. Use select_bucket() beforehead"
        super().__init__(message)


def check_bucket_selected(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        if not self._selected_bucket:
            raise BucketIsNotSelectedException()
        return func(self, *args, **kwargs)

    return inner
