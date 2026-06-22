function createParser(onCommand, onError) {
  let buffer = "";

  function parse(chunk) {
    buffer += chunk.toString();

    const lines = buffer.split("\n");

    // Keep incomplete command in the buffer
    buffer = lines.pop();

    for (const line of lines) {
      const rawMessage = line.trim();

      if (!rawMessage) {
        continue;
      }

      try {
        const command = JSON.parse(rawMessage);
        onCommand(command);
      } catch (err) {
        onError(rawMessage, err);
      }
    }
  }

  return {
    parse,
  };
}

module.exports = {
  createParser,
};