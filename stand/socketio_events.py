import datetime
import json
import logging

from flask_babel import gettext
from stand.factory import create_socket_io_app, create_redis_store


class StandSocketIO:
    def __init__(self, _app):
        self.namespace = '/stand'
        self.socket_io = None
        self.socket_app = None
        self.socket_io, self.socket_app = create_socket_io_app(_app)
        self.logger = logging.getLogger(__name__)
        self.redis_store = create_redis_store(_app)

        handlers = {
            'connect': self.on_connect,
            'disconnect': self.on_disconnect,
            'disconnect request': self.on_disconnect_request,

            'join': self.on_join_room,
            'leave': self.on_leave_room,
            'close': self.on_close_room,
            'echo': self.on_echo,

        }
        for event, handler in list(handlers.items()):
            self.socket_io.on(event, namespace=self.namespace, handler=handler)

    def on_echo(self, sid, message):
        print('=' * 20, ' echo ')
        print(message)
        print('=' * 20)
        self.socket_io.emit('echo', 'Echo: ' + message, room="echo",
                namespace=self.namespace)

    def on_join_room(self, sid, message):
        print('=== > ', message)
        room = str(message.get('room'))

        self.redis_store.hset(
            'room_{}'.format(room), sid,
            json.dumps({'joined': datetime.datetime.utcnow().isoformat()}))

        self.redis_store.expire('room_{}'.format(room), 3600)
        if room.isdigit():
            self.redis_store.expire('cache_room_{}'.format(room), 600)

        self.logger.info(gettext('[%s] joined room %s'), sid, room)
        self.socket_io.enter_room(sid, room, namespace=self.namespace)

        self.socket_io.emit(
            'response', {'message': gettext('Entered room: *{}*').format(room)},
            room=sid, namespace=self.namespace)

        # # Resend all statuses
        cached = self.redis_store.lrange('cache_room_{}'.format(room), 0, -1)
        for msg in cached:
            msg = json.loads(msg)
            self.socket_io.emit(msg['event'], msg['data'], room=sid,
                                namespace=self.namespace)

    def on_leave_room(self, sid, message, connected=True):
        room = str(message.get('room'))

        info = json.loads(
            self.redis_store.hget('room_{}'.format(room), sid) or '{}')
        try:
            info['left'] = datetime.datetime.utcnow().isoformat()
            # self.redis_store.hset('room_{}'.format(room), sid, info)

            self.logger.info(gettext('[%s] left room %s'), sid, room)
            self.socket_io.leave_room(sid, room,
                                      namespace=self.namespace)
            if connected:
                self.socket_io.emit(
                    'response',
                    {'message': gettext('Left room: {}').format(room)},
                    room=sid, namespace=self.namespace)
            self.redis_store.expire('room_{}'.format(room), 10)
        except Exception as e:
            print('ERROR!!!!!', e.message)

    def on_close_room(self, sid, message):
        room = str(message.get('room'))
        self.logger.info(gettext('%s is closing room %s'), sid, room)
        self.socket_io.emit(
            'response', {'message': gettext('Room closed: {}').format(room)},
            room=room,
            namespace=self.namespace)
        self.socket_io.close_room(room, namespace=self.namespace)

    def on_connect(self, sid, message):
        self.logger.info(gettext('%s connected'), sid)
        self.logger.info(message)
        self.socket_io.emit('response',
                            {'message': gettext('Connected'), 'count': 0},
                            room=sid, namespace=self.namespace)

    def on_disconnect(self, sid):
        for room_id in self.socket_io.rooms(sid, self.namespace):
            if room_id.isdigit():
                self.on_leave_room(sid, {'room': room_id}, False)
        self.logger.info(gettext('%s disconnected'), sid)

    def on_disconnect_request(self, sid):
        self.logger.info(gettext('%s asked for disconnection'), sid)
        self.socket_io.disconnect(sid, namespace=self.namespace)
