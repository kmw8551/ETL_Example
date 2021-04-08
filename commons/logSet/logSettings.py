import logging

class Loggers:

    def __init__(self):
        self.record_log = None

    def log(self, name, directory, filenames):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # formatter 및 handler 설정
        formatter = logging.Formatter(
            fmt='%(asctime)s:%(levelname)s:%(message)s',
            datefmt='%Y-%m-%d %I:%M:%S %p',
            style='%'
        )
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        console.setFormatter(formatter)

        file_handler = logging.FileHandler(directory + filenames)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        logger.addHandler(console)
        logger.addHandler(file_handler)
        self.record_log =logger
        return self.record_log







