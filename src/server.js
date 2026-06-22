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

const PORT = 7070;
const HOST = "127.0.0.1";

const server = net.createServer((socket) => {
  const connectionId = addClient(socket);

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

  socket.on("end", () => {
    updateLastSeen(connectionId);
    removeClient(connectionId);

    console.log("Socket disconnected:", connectionId);
    console.log("Total connected clients:", getClientCount());
  });

  socket.on("error", (err) => {
    updateLastSeen(connectionId);
    removeClient(connectionId);

    console.log("Socket error from:", connectionId);
    console.log("Error:", err.message);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`TCP broker running on ${HOST}:${PORT}`);
});