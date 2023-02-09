class Script:
    def __init__(self, content: str) -> None:
        self.content = content

    def render(self):
        return self.content


class SparkRunner:
    def __init__(self, spark_session) -> None:
        self.spark_session = spark_session

    def run(self, script: Script):
        return self.spark_session.sql(script.render())
