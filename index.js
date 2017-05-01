var RedisSMQ = require("rsmq")

var rsmqConsumer = () => {

  var create = (config) => {
    //config
    var rsmq = new RedisSMQ({host: config.host, port: config.port, ns: config.ns})
    var queueName = config.queueName
    var handleMessage = config.handleMessage
    var noMessageWaitTime = config.noMessageWaitTime
    var visibilityOnFail = config.visibilityOnFail

    //control
    var eventLoopShouldRun = false

    var stop = () => {
      eventLoopShouldRun = false
    }

    var start = () => {
      if (!eventLoopShouldRun) {
        eventLoopShouldRun = true
        setTimeout(eventLoop)
      }
    }

    var eventLoop = () => {
      var eventLoopFunction = eventLoop
      if (!eventLoopShouldRun) {
        eventLoopFunction = () => {}
      }
      rsmq.receiveMessage({qname:queueName}, function (err, message) {
        if (err) {
          setTimeout(eventLoopFunction, noMessageWaitTime * 1000)
          return
        }
        if (message.id) {
          var success = () => {
            rsmq.deleteMessage({qname:queueName, id:message.id}, function (err, resp) {
              setTimeout(eventLoopFunction)
            });
          }

          var fail = () => {
            rsmq.changeMessageVisibility({qname:queueName, id:message.id, vt: visibilityOnFail}, function (err, resp) {
              setTimeout(eventLoopFunction)
            });
          }

          setTimeout(() => {
            handleMessage(JSON.parse(message.message), success, fail, message)
          })

        } else {
          setTimeout(eventLoopFunction, noMessageWaitTime * 1000)
        }
      })
    }
    return({start, stop})
  }
  return {create}
}

module.exports = rsmqConsumer()
