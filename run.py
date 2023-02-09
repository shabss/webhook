
from webhook.webhook import WebHookProxy

if __name__ == '__main__':
    webhook = WebHookProxy()
    webhook.start()
    webhook.wait_for_signal()
    webhook.stop()
