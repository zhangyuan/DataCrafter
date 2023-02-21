import jinja2


class Context:
    def __init__(self, params: dict, template_directory: str = None) -> None:
        self.params = params
        self.template_directory = template_directory


class Macro:
    pass


class Script:
    def __init__(self, content: str = None, filepath: str = None) -> None:
        if content is None and filepath is None:
            raise RuntimeError("invalid arguments.")
        self.content = content
        self.filepath = filepath


class Compiler:
    def compile(self, template, params):
        j2_template = jinja2.Template(template, undefined=jinja2.StrictUndefined)
        return j2_template.render(params=params)

    def compile_file(self, template_directory, template_name, params):
        loader = jinja2.FileSystemLoader(template_directory)
        environment = jinja2.Environment(loader=loader)

        template = environment.get_template(template_name)
        return template.render(params=params)


class SparkRunner:
    def __init__(self, spark_session, compiler=None) -> None:
        self.compiler = compiler if compiler is not None else Compiler()
        self.spark_session = spark_session

    def execute_template(self, template: str, context: Context = None):
        params = context.params if context else {}
        statement = self.compiler.compile(template, params=params)
        return self.spark_session.sql(statement)

    def execute_file(self, template_filepath, context: Context = None):
        params = context.params if context else {}
        statement = self.compiler.compile_file(
            context.template_directory, template_filepath, params=params
        )
        return self.spark_session.sql(statement)
