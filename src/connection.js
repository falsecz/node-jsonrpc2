var util = require('util');
var events = require('events');

var Endpoint = require('./endpoint.js');

var Connection = function Connection(ep) {
	events.EventEmitter.call(this);

	this.endpoint = ep;
	this.callbacks = {};
	var self = this;
	this.latestId = 0;

	// Default error handler (prevents "uncaught error event")
	this.on('error', function () {
	});
};

util.inherits(Connection, events.EventEmitter);

/**
 * Make a standard RPC call to the other endpoint.
 *
 * Note that some ways to make RPC calls bypass this method, for example HTTP
 * calls and responses are done in other places.
 */
Connection.prototype.call = function call(method, params, callback) {
	if (!Array.isArray(params)) {
		params = [params];
	}

	var id = null;
	if ("function" === typeof callback) {
		id = ++this.latestId;
		this.callbacks[id] = callback;
	}
	// console.log("cb count", Object.keys(this.callbacks).length);

	var self = this;
	this.timer = setTimeout(function(){
		if(self.callbacks[id]) {
			console.log("timeout: Uklizim po ", method);

			try {
				 self.callbacks[id]("Active timeout on " + method, {});
				 delete self.callbacks[id];
			} catch (err) {
				console.log("error from callback", err, err.stack);
			}
			delete self.callbacks[id];
		}
	}, 300 * 1000)

	Endpoint.trace('-->', 'Connection call (method ' + method + '): ' + JSON.stringify(params));
	var data = JSON.stringify({
		method: method,
		params: params,
		id: id
	});
	this.write(data);
};

/**
 * Dummy method for sending data.
 *
 * Connection types that support sending additional data will override this
 * method.
 */
Connection.prototype.write = function write(data) {
	throw new Error("Tried to write data on unsupported connection type.");
};

/**
 * Keep the connection open.
 *
 * This method is used to tell a HttpServerConnection to stay open. In order
 * to keep it compatible with other connection types, we add it here and make
 * it register a connection end handler.
 */
Connection.prototype.stream = function (onend) {
	if ("function" === typeof onend) {
		this.on('end', onend);
	}
};

Connection.prototype.handleMessage = function handleMessage(msg) {
	var self = this;

	clearTimeout(this.timer);

	if (msg.hasOwnProperty('result') ||
		msg.hasOwnProperty('error') &&
			msg.hasOwnProperty('id') &&
			"function" === typeof this.callbacks[msg.id]) {
		try {
			if(this.callbacks[msg.id]) {
				this.callbacks[msg.id](msg.error, msg.result);
				delete this.callbacks[msg.id];
			}
		} catch (err) {
			console.log("error from callback2", err, err.stack);
			// TODO: What do we do with erroneous callbacks?
		}
	} else if (msg.hasOwnProperty('method')) {
		this.endpoint.handleCall(msg, this, (function (err, result) {
			if (err) {
				if (self.listeners('error').length) {
					self.emit('error', err);
				}
				Endpoint.trace('-->', 'Failure (id ' + msg.id + '): ' +
					(err.stack ? err.stack : err.toString()));
			}

			if ("undefined" === msg.id || null === msg.id) return;

			if (err) {
				err = err.toString();
				result = null;
			} else {
				Endpoint.trace('-->', 'Response (id ' + msg.id + '): ' +
					JSON.stringify(result));
				err = null;
			}

			this.sendReply(err, result, msg.id);
		}).bind(this));
	}
};

Connection.prototype.sendReply = function sendReply(err, result, id) {
	var data = JSON.stringify({
		result: result,
		error: err,
		id: id
	});
	this.write(data);
};

module.exports = Connection
