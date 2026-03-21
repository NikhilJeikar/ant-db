class APIError(BaseException):
    def __init__(self, *args, **kwargs):
        print(args, kwargs)