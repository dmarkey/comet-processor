import datetime

__author__ = 'dmarkey'
import json
import redis


pool = r = None


class Config(object):
    def __init__(self):
        self.REDIS_HOST = "localhost"
        self.REDIS_PORT = 6379
        self.REDIS_NAMESPACE = "/comet_listener/"

comet_config = Config()


def init():
    global pool, r
    if pool is not None:
        return
    pool = redis.ConnectionPool(host=comet_config.REDIS_HOST, port=comet_config.REDIS_PORT)
    r = redis.Redis(connection_pool=pool)

date_handler = lambda obj: (
    obj.isoformat()
    if isinstance(obj, datetime.datetime)
    or isinstance(obj, datetime.date)
    else None
)


class TalkBackEvent(object):
    def __init__(self, request):
        self.request_data = request
        self.event_type = request['event']

    @staticmethod
    def from_session_id(session_id):
        """
        Use this static method to create a TalkBackEvent to send a message to the consumer.
        :param session_id: the required session_id
        :return: A TalkBackEvent object
        """
        return TalkBackEvent({'id': session_id, "event": "push"})

    @staticmethod
    def send_message_bulk(session_ids, message, status=200):
        """
        Given a list of session ids, send the same message back to each consumer.
        :param session_ids: list of session_ids
        :param message: the message thats required (object thats JSON serializable)
        :param status: HTTP status code
        :return:
        """
        for session_id in session_ids:
            TalkBackEvent.from_session_id(session_id).send_message(message, status)

    def serialize(self):
        """
        Serialize this object into JSON
        :return: JSON object
        """
        return json.dumps(self.request_data)

    def get_session_id(self):
        """
        Get the uuid of this object
        :return: the UUID (String form)
        """
        return self.request_data['id']

    def send_message(self, message="", status=200):
        """
        Send a message to the TalkBack client
        :param message: the message to be sent
        :param status: the HTTP status code
        :return:
        """
        init()
        wrapper = {'status_code': status, 'message': message, "id": self.get_session_id()}
        payload = json.dumps(wrapper, default=date_handler)
        session_id = self.get_session_id()
        script = """
            local queue = redis.call('hget', KEYS[3] .. 'active_sessions', KEYS[1])
            if queue == '' then
                redis.log(redis.LOG_DEBUG, "BACKLOG : No Queue, pushing to backlog " .. KEYS[1])
                redis.call("lpush", KEYS[3] .. KEYS[1] .. "_backlog", KEYS[2])
                return "BACKLOG"
            elseif queue == nil then
                redis.log(redis.LOG_DEBUG, "TIMEOUT : Timed out " .. KEYS[1])
                return "TIMEOUT"
            end
            local result = redis.call("publish", queue, KEYS[2])
            if result == 0 then
                redis.log(redis.LOG_DEBUG, "BACKLOG_FAILURE : PUB Failed, saving to backlog  " .. KEYS[1])
                redis.call("lpush", KEYS[3] ..  KEYS[1] .. "_backlog", KEYS[2])
                return "BACKLOG_FAILURE"
            end
            redis.log(redis.LOG_DEBUG, "SUCCESS : " .. KEYS[1])
            return "SUCCESS"
        """
        return r.eval(script, 3, session_id, payload, comet_config.REDIS_NAMESPACE)

    def finish(self):
        """
        Finish the session
        :return: nothing
        """
        self.send_message("session closed", status=410)

    def unauthorized(self, message=None):
        """
        Use this when a client is or has become unauthorized
        :param message: Message to give if any
        :return:
        """
        return self.send_message(message, status=401)

    def bad_request(self, message=None):
        """
        Use this when the client has given a bad request
        :param message: Message to give if any
        :return: Nothing
        """
        return self.send_message(message, status=400)


class IncomingProcessor(object):
    service_name = None

    @staticmethod
    def deserialize(payload):
        return TalkBackEvent(request=json.loads(payload))

    def __init__(self):
        if not self.service_name:
            raise Exception("Please subclass and define service_name")

    def process_incoming(self, item):
        try:
            incoming = json.loads(item.decode("utf-8"))
        except TypeError:
            return

        request = TalkBackEvent(incoming)

        self.work(request)

    def work(self, request):
        """
        Subclass this to consume each of the events coming in.
        :param request: The incoming request in the form of a TalkBackEvent
        :return: Nothing
        """
        raise NotImplementedError

    def run(self):
        """
        Execute this to start the processing
        :return: nothing
        """
        init()
        list_name = comet_config.REDIS_NAMESPACE + "incoming/" + self.service_name
        list_name_processing = list_name + "/processing"
        self.redis = r
        while True:
            try:
                item = self.redis.brpoplpush(list_name,  list_name_processing)
                self.process_incoming(item)
                self.redis.lrem(list_name_processing, item)

            except redis.ConnectionError:
                pass
