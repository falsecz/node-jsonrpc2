var net = require('net');
var http = require('http');
var util = require('util');
var events = require('events');
var JsonParser = require('jsonparse');

var Endpoint = require('./endpoint.js');
var SocketConnection = require('./socket-connection.js');

/**
 * JSON-RPC Client.
 */
var Client = function (port, host, user, password)
{
	Endpoint.call(this);

	this.port = port;
	this.host = host;
	this.user = user;
	this.password = password;
};

util.inherits(Client, Endpoint);


/**
 * Make HTTP connection/request.
 *
 * In HTTP mode, we get to submit exactly one message and receive up to n
 * messages.
 */
Client.prototype.connectHttp = function connectHttp(method, params, opts, callback)
{
	if ("function" === typeof opts) {
		callback = opts;
		opts = {};
	}
	opts = opts || {};

	var id = 1;

	// First we encode the request into JSON
	var requestJSON = JSON.stringify({
		'id': id,
		'method': method,
		'params': params,
		'jsonrpc': '2.0'
	});

	var headers = {};

	if (this.user && this.password) {
		var buff = new Buffer(this.user + ":" + this.password).toString('base64');
		var auth = 'Basic ' + buff;
		headers['Authorization'] = auth;
	}

	// Then we build some basic headers.
	headers['Host'] = this.host;
	headers['Content-Length'] = Buffer.byteLength(requestJSON, 'utf8');

	// Now we'll make a request to the server
	var options = {
		hostname: this.host,
		port: this.port,
		path: opts.path || '/',
		method: 'POST',
		headers: headers
	};
	var request = http.request(options);

	// Report errors from the http client. This also prevents crashes since
	// an exception is thrown if we don't handle this event.
	request.on('error', function(err) {
		callback(err);
	});
	request.write(requestJSON);
	request.on('response', callback.bind(this, id, request));
};

/**
 * Make Socket connection.
 *
 * This implements JSON-RPC over a raw socket. This mode allows us to send and
 * receive as many messages as we like once the socket is established.
 */
Client.prototype.connectSocket = function connectSocket(callback)
{
	var self = this;

	var socket = net.connect(this.port, this.host, function () {
		// Submit non-standard "auth" message for raw sockets.
		if ("string" === typeof self.user &&
			"string" === typeof self.password) {
			conn.call("auth", [self.user, self.password], function (err) {
				if (err) {
					callback(err);
				} else {
					callback(null, conn);
				}
			});
			return;
		}
		if ("function" === typeof callback) {
			callback(null, conn);
		}
	});
	var conn = new SocketConnection(self, socket);
	var parser = new JsonParser();
	parser.onValue = function (decoded) {
		if (this.stack.length) return;

		conn.handleMessage(decoded);
	};
	socket.on('data', function (chunk) {
		try {
			parser.write(chunk);
		} catch(err) {
			Endpoint.trace('<--', err.toString());
		}
	});

	return conn;
};

Client.prototype.stream = function (method, params, opts, callback)
{
	if ("function" === typeof opts) {
		callback = opts;
		opts = {};
	}
	opts = opts || {};

	this.connectHttp(method, params, opts, function (id, request, response) {
		if ("function" === typeof callback) {
			var connection = new events.EventEmitter();
			connection.id = id;
			connection.req = request;
			connection.res = response;
			connection.expose = function (method, callback) {
				connection.on('call:'+method, function (data) {
					callback.call(null, data.params || []);
				});
			};
			connection.end = function () {
				this.req.connection.end();
			};

			// We need to buffer the response chunks in a nonblocking way.
			var parser = new JsonParser();
			parser.onValue = function (decoded) {
				if (this.stack.length) return;

				connection.emit('data', decoded);
				if (decoded.hasOwnProperty('result') ||
					decoded.hasOwnProperty('error') &&
						decoded.id === id &&
						"function" === typeof callback) {
					connection.emit('result', decoded);
				} else if (decoded.hasOwnProperty('method')) {
					connection.emit('call:'+decoded.method, decoded);
				}
			};
			// Handle headers
			connection.res.once('data', function (data) {
				if (connection.res.statusCode === 200) {
					callback(null, connection);
				} else {
					callback(new Error(""+connection.res.statusCode+" "+data));
				}
			});
			connection.res.on('data', function (chunk) {
				try {
					parser.write(chunk);
				} catch(err) {
					// TODO: Is ignoring invalid data the right thing to do?
				}
			});
			connection.res.on('end', function () {
				// TODO: Issue an error if there has been no valid response message
			});
		}
	});
};

Client.prototype.call = function (method, params, opts, callback)
{
	if ("function" === typeof opts) {
		callback = opts;
		opts = {};
	}
	opts = opts || {};
	Endpoint.trace('-->', 'Http call (method '+method+'): ' + JSON.stringify(params));
	this.connectHttp(method, params, opts, function (id, request, response) {
		var data = '';
		response.on('data', function (chunk) {
			data += chunk;
		});
		response.on('end', function () {
			if (response.statusCode !== 200) {
				callback(new Error(""+response.statusCode+" "+data));
				return;
			}
			var decoded = JSON.parse(data);
			if ("function" === typeof callback) {
				if (!decoded.error) {
					decoded.error = null;
				}
				callback(decoded.error, decoded.result);
			}
		});
	});
};

module.exports = Client
