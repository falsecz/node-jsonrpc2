var util = require('util');

var Connection = require('./connection.js');


/**
 * Socket connection.
 *
 * Socket connections are mostly symmetric, so we are using a single class for
 * representing both the server and client perspective.
 */
var SocketConnection = function (endpoint, conn) {
	Connection.call(this, endpoint);

	var self = this;

	this.conn = conn;
	this.autoReconnect = true;
	this.ended = true;

	this.conn.on('connect', function () {
		self.emit('connect');
	});

	this.conn.on('end', function () {
		self.emit('end');
	});

	this.conn.on('error', function () {
		self.emit('error');
	});

	this.conn.on('close', function (hadError) {
		self.emit('close', hadError);

		// Handle automatic reconnections if we are the client
		var Client = require('./client.js');
		if (self.endpoint instanceof Client &&
			self.autoReconnect &&
			!self.ended) {
			if (hadError) {
				// If there was an error, we'll wait a moment before retrying
				setTimeout(self.reconnect.bind(self), 200);
			} else {
				self.reconnect();
			}
		}
	});
};

util.inherits(SocketConnection, Connection);

SocketConnection.prototype.write = function write(data) {
	if (!this.conn.writable) {
		// Other side disconnected, we'll quietly fail // WAT
		console.log('Error: Attempted to write data to non-writable socket.');
		this.conn.emit('close', 'Socket is not writable anymore.');
		return;
	}

	this.conn.write(data);
};

SocketConnection.prototype.end = function end() {
	this.ended = true;
	this.conn.end();
};

SocketConnection.prototype.reconnect = function reconnect() {
	this.ended = false;
	if (this.endpoint instanceof Client) {
		this.conn.connect(this.endpoint.port, this.endpoint.host);
	} else {
		throw new Error('Cannot reconnect a connection from the server-side.');
	}
};

module.exports = SocketConnection;
