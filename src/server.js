const net = require("net");

const { createParser } = require("./parser");
const { handleCommand } = require("./commands/handle.command");

const {
  addClient,
  getClient,
  updateLastSeen,
  removeClient,
  getClientCount,
} = require("./clients/clients.store");

const { send } = require("./utils/response");
const {
  removeClientFromSubscriptions,
} = require("./subscriptions/subscriptions.store");
const { startDeliveryEngine } = require("./delivery/delivery.engine");

const PORT = 7070;
const HOST = "127.0.0.1";

const server = net.createServer((socket) => {
  const connectionId = addClient(socket);
  let cleanedUp = false;

  console.log("Socket connected:", connectionId);
  console.log("Client info:", getClient(connectionId));
  console.log("Total connected clients:", getClientCount());

  send(socket, {
    type: "WELCOME",
    message: "Welcome to the TCP broker. Send CONNECT to begin.",
    connectionId,
  });

  const parser = createParser(
    (command) => {
      console.log("Received command:", command);
      handleCommand(socket, connectionId, command);
    },

    (rawMessage) => {
      updateLastSeen(connectionId);

      send(socket, {
        type: "ERROR",
        message: "Invalid JSON command",
        rawMessage,
      });
    }
  );

  socket.on("data", (chunk) => {
    parser.parse(chunk);
  });

  function cleanupClient(reason) {
    if (cleanedUp) {
      return;
    }

    cleanedUp = true;

    updateLastSeen(connectionId);
    removeClientFromSubscriptions(connectionId);
    removeClient(connectionId);

    console.log(`Socket ${reason}:`, connectionId);
    console.log("Total connected clients:", getClientCount());
  }

  socket.on("end", () => {
    cleanupClient("ended");
  });

  socket.on("error", (err) => {
    console.log("Socket error from:", connectionId);
    console.log("Error:", err.message);
  });

  socket.on("close", () => {
    cleanupClient("closed");
  });
});

server.listen(PORT, HOST, () => {
  startDeliveryEngine();
  console.log(`TCP broker running on ${HOST}:${PORT}`);
});
