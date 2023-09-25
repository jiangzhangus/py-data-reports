from logging import Logger
import logging
import os


class _Logger(Logger):  # pragma: no cover

    # log_format = '%(asctime)s.%(msecs)d [%(levelname)s] "%(message)s"'
    log_format = '%(asctime)s [%(levelname)s] "%(message)s"'
    date_format = '%Y-%m-%d %H:%M:%S'

    def __init__(self, name):
        super(_Logger, self).__init__(name)

        # set up log
        log_level = None
        if 'LOG_LEVEL' in os.environ:
            level = os.environ['LOG_LEVEL']
        else:
            level = "INFO"

        if level == "DEBUG":
            log_level = logging.DEBUG
        elif level == "INFO":
            log_level = logging.INFO
        elif level == "WARN":
            log_level = logging.WARN
        elif level == "ERROR":
            log_level = logging.ERROR
        elif level == "FATAL":
            log_level = logging.FATAL

        self.setLevel(log_level)
        ch = logging.StreamHandler()
        self.addHandler(ch)

        for hand in [h for h in self.handlers]:
            hand.setFormatter(logging.Formatter(self.log_format, datefmt=self.date_format))

    def set_level_to_debug(self) -> None:
        return super().setLevel(logging.DEBUG)


logger = _Logger("python-script")
