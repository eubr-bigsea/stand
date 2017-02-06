import logging

from stand.factory import create_socket_io_app

logger = logging.getLogger(__name__)


class StandSocketIO:
    def __init__(self, _app):
        self.namespace = '/stand'
        self.socket_io = None
        self.socket_app = None
        self.socket_io, self.socket_app = create_socket_io_app(_app)

        handlers = {
            'connect': self.on_connect,
            'disconnect': self.on_disconnect,
            'disconnect request': self.on_disconnect_request,

            'join': self.on_join_room,
            'leave': self.on_leave_room,
            'close': self.on_close_room,

        }
        for event, handler in handlers.items():
            self.socket_io.on(event, namespace=self.namespace, handler=handler)

    def on_join_room(self, sid, message):
        logger.debug('%s joined room %s', sid, message['room'])
        self.socket_io.enter_room(sid, message['room'],
                                  namespace=self.namespace)
        self.socket_io.emit(
            'response', {'msg': 'Entered room: {}'.format(message['room'])},
            room=sid, namespace=self.namespace)

    def on_leave_room(self, sid, message):
        logger.debug('%s left room %s', sid, message.get('room'))
        self.socket_io.leave_room(sid, message['room'],
                                  namespace=self.namespace)
        self.socket_io.emit(
            'response', {'msg': 'Left room: {}'.format(message.get('room'))},
            room=sid, namespace=self.namespace)

    def on_close_room(self, sid, message):
        logger.debug('%s is closing room %s', sid, message.get('room'))
        self.socket_io.emit(
            'response', {'msg': 'Room closed: {}'.format(message.get('room'))},
            room=message['room'], namespace=self.namespace)
        self.socket_io.close_room(message.get('room'), namespace=self.namespace)

    def on_connect(self, sid, message):
        logger.debug('%s connected', sid)
        logger.debug(message)
        self.socket_io.emit('response', {'msg': 'Connected', 'count': 0},
                            room=sid, namespace=self.namespace)

    def on_disconnect(self, sid):
        self.socket_io.disconnect(sid, namespace=self.namespace)
        logger.debug('%s disconnected', sid)

    def on_disconnect_request(self, sid):
        logger.debug('%s asked for disconnection', sid)
        self.socket_io.disconnect(sid, namespace=self.namespace)
