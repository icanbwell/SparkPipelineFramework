class VendorResponseKeyError(Exception):
    """
    shows that something is wrong with the vendor and we should not continue calling it
    """

    def __init__(
        self,
        message: str = "Too many errors calling Address Standardization api,"
        " please check prior errors in the log for more details",
    ) -> None:
        self.message = message
        super().__init__(self.message)
