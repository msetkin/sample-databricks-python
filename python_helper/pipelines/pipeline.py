class Pipeline:
    def __init__(self) -> None:
        """
        This constructor is empty because this is an interface, any implementation is not required.
        """
        pass

    def define_bronze_pipelines(self) -> None:
        """
        This method is empty because this is an interface, any implementation is not required.
        """
        pass

    def define_silver_pipelines(self) -> None:
        """
        This method is empty because this is an interface, any implementation is not required.
        """
        pass

    def define_pipelines(self) -> None:
        self.define_bronze_pipelines()
        self.define_silver_pipelines()
