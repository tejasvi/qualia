from qualia.models import Li, ListenerRequest
from qualia.services.utils.listener_utils import create_listener
from qualia.services.utils.preview_utils import preview_node
from qualia.utils.common_utils import logger, exception_traceback


class RpcListenExternal:
    def __init__(self) -> None:
        self.listener = create_listener()

        with self.listener as listener:
            while True:
                with listener.accept() as conn:
                    self.conn = conn
                    logger.debug('connection accepted from', self.listener.last_accepted)
                    while not self.conn.closed:
                        try:
                            request_params: ListenerRequest = self.conn.recv()
                        except ConnectionResetError:
                            logger.critical("Connection broken")
                            break
                        try:
                            function, args, kwargs = request_params
                        except ValueError as e:
                            logger.critical(f"Invalid number of request paramters {request_params=}")
                            self.safe_send(exception_traceback(e) + str(request_params))
                            break
                        try:
                            rpc_result = getattr(self, function)(*args, **kwargs)
                        except Exception as e:
                            traceback = exception_traceback(e)
                            logger.debug(traceback)
                            self.safe_send(traceback)
                        else:
                            self.safe_send(rpc_result)

    def safe_send(self, data: object) -> None:
        try:
            self.conn.send(data)
        except OSError as e:
            logger.debug(str(e))

    def close_connection(self) -> None:
        self.conn.close()

    @staticmethod
    def preview_node(*args, **kwargs) -> Li:
        return preview_node(*args, **kwargs)
