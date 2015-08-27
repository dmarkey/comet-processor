__author__ = 'dmarkey'
import json
import redis
import sys
sys.path.append(".")
import comet_config

pool = redis.ConnectionPool(host=comet_config.REDIS_HOST, port=comet_config.REDIS_PORT)

r = redis.Redis(connection_pool=pool)


class TalkBackRequest(object):
    def __init__(self, request):
        self.request_data = request

    @staticmethod
    def from_uuid(uuid):
        return TalkBackRequest({'uuid': uuid})

    @staticmethod
    def send_message_bulk(self, uuids, message, status=200):
        for uuid in uuids:
            self.from_uuid(uuid).send_messge(message, status)

    def serialize(self):
        return json.dumps(self.request_data)

    def get_uuid(self):
        return self.request_data['uuid']

    def send_message(self, result, status=200):
        wrapper = {'result_status': status, 'result_payload': result, "uuid": self.get_uuid()}
        payload = json.dumps(wrapper)
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
        self.send_message(message, status=401)


class IncomingProcessor(object):
    service_name = None

    @staticmethod
    def deserialize(payload):
        return TalkBackRequest(request=json.loads(payload))

    def __init__(self):
        if not self.service_name:
            raise Exception("Please subclass and define service_name")
        self.redis = r
        self.list_name = comet_config.REDIS_NAMESPACE + "incoming/" + self.service_name

    def on_failure(self, message):
        pass

    def process_incoming(self, item):
        try:
            incoming = json.loads(item.decode("utf-8"))
        except TypeError:
            return

        request = TalkBackRequest(incoming)

        self.work(request)

    def run(self):
        while True:
            try:
                item = self.redis.blpop(self.list_name)[1]
                self.process_incoming(item)
            except redis.ConnectionError:
                pass
