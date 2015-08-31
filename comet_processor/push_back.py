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


class AcknowledgementNeeded(Exception):
    pass


class TalkBackEvent(object):
    def __init__(self, request):
        self.request_data = request
        self.event_type = request['event']
        if self.event_type == "init":
            self.ack_needed = True
        else:
            self.ack_needed = False

    @property
    def ack_list(self):
        return comet_config.REDIS_NAMESPACE + "ack_waiting/" + self.get_uuid()

    def ack(self, message="", status=200):
        wrapper = {'result_status': status, 'message': message, "uuid": self.get_uuid()}
        r.lpush(self.ack_list, json.dumps(wrapper, default=date_handler))
        self.ack_needed = False

    @staticmethod
    def from_uuid(uuid):
        return TalkBackEvent({'uuid': uuid, "event": "push"})

    @staticmethod
    def send_message_bulk(self, uuids, message, status=200):
        for uuid in uuids:
            self.from_uuid(uuid).send_messge(message, status)

    def serialize(self):
        return json.dumps(self.request_data)

    def get_uuid(self):
        return self.request_data['uuid']

    def send_message(self, result, status=200):
        if self.ack_needed:
            raise AcknowledgementNeeded()
        wrapper = {'result_status': status, 'result_payload': result, "uuid": self.get_uuid()}
        print(wrapper)
        payload = json.dumps(wrapper, default=date_handler)
        uuid = self.get_uuid()
        script = """
            local queue = redis.call('hget', KEYS[3] .. 'active_uuids', KEYS[1])
            if queue == '' then
                redis.call("lpush", KEYS[3] .. KEYS[1] .. "_backlog", KEYS[2])
                return "BACKLOG"
            elseif queue == nil then
                return "TIMEOUT"
            end
            local result = redis.call("publish", queue, KEYS[2])
            if result == 0 then
                redis.call("lpush", KEYS[3] ..  KEYS[1] .. "_backlog", KEYS[2])
                return "BACKLOG"
            end
            return "SUCCESS"
        """
        return r.eval(script, 3, uuid, payload, comet_config.REDIS_NAMESPACE)

    def finish(self):
        self.send_message("session closed", status=410)

    def unauthorized(self, message=None):
        if not self.ack_needed:
            return self.send_message(message, status=401)
        return self.ack(message, status=401)

    def bad_request(self, message=None):
        if not self.ack_needed:
            return self.send_message(message, status=400)
        return self.ack(message, status=400)


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

    def run(self):
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
