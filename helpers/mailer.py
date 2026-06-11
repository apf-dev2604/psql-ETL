class Mailer:
    """Safe placeholder mailer.

    Wire this to utilities.mailer.send_migration_reports in your deployment if
    live email notification is required. It is deliberately no-op here so dry-run
    or test execution cannot unexpectedly send email.
    """

    def send(self, *args, **kwargs) -> bool:
        return False
