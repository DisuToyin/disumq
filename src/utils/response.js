function send(socket, payload) {
  socket.write(JSON.stringify(payload) + "\n");
}

module.exports = {
  send,
};