from apache_beam.options.pipeline_options import PipelineOptions

class CustomBeamOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            help="projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>",
        )

        parser.add_argument(
            "--dead_letter_topic",
            help="projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>",
        )

        parser.add_argument(
            "--mysql_connection_name",
            help="project:region:instance",
        )

        parser.add_argument(
            "--mysql_user",
            help="username",
        )

        parser.add_argument(
            "--mysql_pass",
            help="password",
        ) 

        parser.add_argument(
            "--mysql_db",
            help="database name",
        )

        parser.add_argument(
            "--mysql_database_driver",
            help="projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>",
            default="pymysql"
        )


    