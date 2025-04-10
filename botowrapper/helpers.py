from functools import wraps


class BucketIsNotSelectedException(Exception):
    def __init__(self, message: str | None):
        if not message:
            message = "Bucket is not selected. Use select_bucket() beforehead"
        super().__init__(message)


# ? basicaly could use @Property with right getter and setter,
# ? but there is a bug if initial value of bucket is None
# ? that I couldn't figure way around
def check_bucket_selected(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        if not self._selected_bucket:
            raise BucketIsNotSelectedException()
        return func(self, *args, **kwargs)

    return inner
