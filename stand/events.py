from app import socketio as sio


@sio.on('my event', namespace='/stand')
def test_message(sid, message):
    sio.emit('response', {'msg': message['msg']}, room=sid,
             namespace='/stand')


@sio.on('my broadcast event', namespace='/stand')
def test_broadcast_message(sid, message):
    sio.emit('response', {'msg': message['msg']}, namespace='/stand')


@sio.on('join', namespace='/stand')
def join(sid, message):
    sio.enter_room(sid, message['room'], namespace='/stand')
    sio.emit('response', {'msg': 'Entered room: ' + str(message['room'])},
             room=sid, namespace='/stand')


@sio.on('leave', namespace='/stand')
def leave(sid, message):
    sio.leave_room(sid, message['room'], namespace='/stand')
    sio.emit('response', {'msg': 'Left room: ' + str(message['room'])},
             room=sid, namespace='/stand')


@sio.on('close room', namespace='/stand')
def close(sid, message):
    sio.emit('response',
             {'msg': 'Room ' + message['room'] + ' is closing.'},
             room=message['room'], namespace='/stand')
    sio.close_room(message['room'], namespace='/stand')


@sio.on('my room event', namespace='/stand')
def send_room_message(sid, message):
    sio.emit('response', {'msg': message['msg']}, room=message['room'],
             namespace='/stand')


@sio.on('disconnect request', namespace='/stand')
def disconnect_request(sid):
    sio.disconnect(sid, namespace='/stand')


@sio.on('connect', namespace='/stand')
def test_connect(sid, environ):
    sio.emit('response', {'msg': 'Connected', 'count': 0}, room=sid,
             namespace='/stand')


@sio.on('disconnect', namespace='/stand')
def test_disconnect(sid):
    print('Client disconnected')
