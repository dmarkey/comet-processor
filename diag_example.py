import copy

__author__ = 'dmarkey'
from threading import Timer, Lock
from comet_processor.push_back import IncomingProcessor, TalkBackRequest

lock = Lock()

requests = {}

TOKEN = "MySecretToken"

MESSAGES_TO_DELIVER = 1


class MyProcessor(IncomingProcessor):
    service_name = "diag_example"

    def work(self, item):
        if item.request_data['event'] == "init":
            if item.request_data['data'].get("token", None) != TOKEN:
                print(item.request_data)
                return item.unauthorized("Token incorrect")

            request_serialized = item.get_uuid()
            with lock:
                item.ack()
                requests[request_serialized] = 0


def emulate_push():
    print("Doing a push..")

    global requests
    with lock:
        for req, message_num in copy.copy(requests).items():

            results = {"Results": [{"Severity": "Low"}, {"Severity": "High"}, {"Severity": "Medium"}],
                   "original_request": "<unavailable>"}
            message_num += 1
            results['message_num'] = message_num
            item = TalkBackRequest.from_uuid(req)
            item.send_message(results)

            if message_num == MESSAGES_TO_DELIVER:
                requests.pop(req)
            else:
                requests[req] = message_num

    Timer(30.0, emulate_push).start()


def init():
    Timer(30.0, emulate_push).start()
    MyProcessor().run()


if __name__ == "__main__":
    init()