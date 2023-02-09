import jinja2

class Context():
    def __init__(self, variables: dict) -> None:
        self.variables = variables

class Macro():
    pass

class Script:
    def __init__(self, content: str) -> None:
        self.content = content

class Compiler():
    def compile(self, template, variables):
        j2_template = jinja2.Template(template, undefined=jinja2.StrictUndefined)
        return j2_template.render(var=variables)


class SparkRunner:
    def __init__(self, spark_session, compiler=None) -> None:
        self.compiler = compiler if compiler is not None else Compiler()
        self.spark_session = spark_session

    def run(self, script: Script, context: Context=None):
        variables = context.variables if context else {}
        statement = self.compiler.compile(script.content, variables=variables)
        return self.spark_session.sql(statement)
