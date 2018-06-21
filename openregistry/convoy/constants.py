DEFAULTS = {
    "timeout": 10,
    "transmitter_timeout": 15,
    "db": {
        "login": "",
        "password": "",
        "host": "127.0.0.1",
        "port": "5984",
        "name": "ea_auctions",
    },
    "auctions": {
        "api": {
            "token": "convoy",
            "url": "http://127.0.0.1:6543",
            "version": 0
        },
        "ds": {
            "host_url": "http://docs-sandbox.ea.openprocurement.org",
            "auth_ds": ["", ""]
        }
    },
    "lots": {
        "api": {
            "token": "convoy",
            "url": "http://0.0.0.0:6543",
            "version": 0
        }
    },
    "assets": {
        "api": {
            "token": "convoy",
            "url": "http://0.0.0.0:6543",
            "version": 0
        }
    },
    "version": 1,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        },
        "statsd": {
            "class": "logging.StatsdHandler",
            "level": "DEBUG",
            "config_path": "path/to/statsd/config"
        }
    },
    "loggers": {
        "openregistry.convoy": {
            "handlers": ["console", "statsd"],
            "propagate": "no",
            "level": "DEBUG"
        },
        "logger_exc_logger": {
            "level": "ERROR",
            "handlers": None,
            "qualname": "exc_logger",
            "propagate": 0
        },
        "": {
            "handlers": ["console", "statsd"],
            "level": "DEBUG"
        }
    }
}

DOCUMENT_KEYS = ['hash', 'description', 'title', 'url', 'format', 'documentType']
KEYS = ['classification', 'additionalClassifications', 'address', 'unit', 'quantity', 'location', 'id']