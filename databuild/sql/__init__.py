import jinja2


class Context:
    def __init__(self, params: dict) -> None:
        self.params = params


class Macro:
    pass


class Script:
    def __init__(self, content: str) -> None:
        self.content = content


class Compiler:
    def compile(self, template, params):
        j2_template = jinja2.Template(template, undefined=jinja2.StrictUndefined)
        return j2_template.render(params=params)


class SparkRunner:
    def __init__(self, spark_session, compiler=None) -> None:
        self.compiler = compiler if compiler is not None else Compiler()
        self.spark_session = spark_session

    def run(self, script: Script, context: Context = None):
        params = context.params if context else {}
        statement = self.compiler.compile(script.content, params=params)
        return self.spark_session.sql(statement)
