import jinja2


class Context:
    def __init__(self, params: dict = None, template_directory: str = None) -> None:
        self.params = params if params else {}
        self.template_directory = template_directory


class Compiler:
    def compile(self, template, params, helper=None):
        j2_template = jinja2.Template(template, undefined=jinja2.StrictUndefined)
        return j2_template.render(params=params, helper=helper)

    def compile_file(self, template_directory, template_name, params, helper=None):
        loader = jinja2.FileSystemLoader(template_directory)
        environment = jinja2.Environment(loader=loader, undefined=jinja2.StrictUndefined)

        template = environment.get_template(template_name)
        return template.render(params=params, helper=helper)


class Helper:
    def __init__(self, spark_session) -> None:
        self.spark_session = spark_session

    def star(self, from_table, except_columns=None):
        df = self.spark_session.sql(f"SHOW COLUMNS IN {from_table}")
        columns = list(x[0] for x in df.collect())
        if except_columns:
            if not isinstance(except_columns, list):
                except_columns = [except_columns]
            columns = list(x for x in columns if x not in except_columns)
        return ", ".join(columns)


class SparkRunner:
    def __init__(self, spark_session, compiler=None) -> None:
        self.compiler = compiler if compiler is not None else Compiler()
        self.spark_session = spark_session
        self.helper = Helper(spark_session)

    def execute_template(self, template: str, context: Context = None):
        params = context.params if context else {}
        statement = self.compiler.compile(template, params=params, helper=self.helper)
        return self.spark_session.sql(statement)

    def execute_template_path(self, template_path, context: Context = None):
        params = context.params if context else {}
        statement = self.compiler.compile_file(
            context.template_directory, template_path, params=params, helper=self.helper
        )
        return self.spark_session.sql(statement)
