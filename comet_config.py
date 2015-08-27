__author__ = 'dmarkey'

SERVICES = {
    "diag_example": {"timeout": 3600}
}

REDIS_HOST = "0.0.0.0"
REDIS_PORT = 6379
REDIS_NAMESPACE = "/comet_listener/"

LISTENER_PORT = 8080
LISTENER_ADDRESS = "0.0.0.0"
LISTENING_PROCESSES = 4

URL_PREFIX = "/comet_server"


