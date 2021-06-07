class FrameworkMappingRunnerException(Exception):
    def __init__(self, msg: str, exception: Exception) -> None:
        self.exception: Exception = exception
        super().__init__(f"{msg}: \n{exception}")
