const express = require('express');
const bodyParser = require('body-parser');
const DHT = require("@hyperswarm/dht");
const crypto = require('hypercore-crypto');
const { unpack, pack } = require('msgpackr');
const Keychain = require('keypear');

const app = express();
app.use(bodyParser.json());

const node = new DHT();
const clusterState = {
  nodes: [],
  namespaces: [],
  pods: [],
  deployments: [],
  services: [],
  daemonsets: [],
  replicasets: [],
};

const handlers = {
  createCluster: async (args) => {
    const { network, role } = args;
    const keys = new Keychain(crypto.keyPair());
    const keyPair = keys.get('network');

    // Make the current node serve as the cluster creator (master)
    const cluster = node.createServer({ reusableSocket: true });
    cluster.on("connection", function (socket) {
      console.log('connection', keyPair.publicKey.toString('hex'));
      socket.on('error', function (e) { throw e });
      socket.on("data", async data => {
        try {
            socket.write("connected");
        } catch (error) {
          console.trace(error);
          socket.write(pack({ error }));
        }
        socket.end();
      });
    });
    cluster.listen(keyPair);

    const result = `Created a ${role} cluster on network ${network} with cluster key ${keyPair.publicKey.toString('hex')}`;

    return result;
  },

  joinCluster: async (args) => {
    const { network, role, masterKey } = args;

    console.log('run', args);
    const publicKey = args.masterKey
      const keys = new Keychain(publicKey)
      const key = keys.sub(network).publicKey;
      console.log({keys, key})
      return runKey(masterKey, args), `Joined a ${role} cluster on network ${network}.`;
  }
};

const runKey = async (command, key, args) => {
    return new Promise((pass, fail) => {
      const keyArray = Uint8Array.from(key, c => c.charCodeAt(0)); // Convert key to Uint8Array
  
      console.log('calling', key.toString('hex'), args);
      const socket = node.connect(keyArray, { reusableSocket: true });
      socket.on('open', function () {
        console.log('socket opened');
        socket.write("connected");
      });
      socket.on("data", (res) => {
        socket.end();
        const out = unpack(res);
        if (out && out.error) {
          fail(out.error);
          throw out;
        }
        pass(out);
      });
      socket.on('error', error => fail({ error }));
      socket.write(pack(args));
    });
  };
  

  function handleRequest({ command, args }) {
    if (!handlers[command]) {
      throw new Error(`Unknown command: ${command}`);
    }
  
    return handlers[command](args)
      .catch((error) => {
        console.error(`Error occurred during command '${command}': ${error.message}`);
        throw error;
      });
  }
  

app.post('/createCluster', async (req, res) => {
  const { network, role } = req.body;
  console.log(`Received a request to create a cluster. Network: ${network}, Role: ${role}`);
  try {
    const response = await handleRequest({ command: 'createCluster', args: { network, role } });
    console.log(`Cluster created successfully. Response: ${JSON.stringify(response)}`);
    res.json(response);
  } catch (error) {
    console.error(`Error occurred while creating a cluster: ${error.message}`);
    res.status(500).json({ error: error.message });
  }
});

app.post('/joinCluster', async (req, res) => {
  const { network, role, masterKey } = req.body;
  console.log(`Received a request to join a cluster. Network: ${network}, Role: ${role}`);
  try {
    const response = await handleRequest({ command: 'joinCluster', args: { network, role, masterKey: Buffer.from(masterKey, 'hex') } });
    console.log(`Successfully joined the cluster. Response: ${JSON.stringify(response)}`);
    res.json(response);
  } catch (error) {
    console.error(`Error occurred while joining a cluster: ${error.message}`);
    res.status(500).json({ error: error.message });
  }
});

app.listen(3008, () => console.log('App listening on port 3008'));

async function exitHandler(options, exitCode) {
  if (options.cleanup) console.log('DHT Cleanup');
  if (exitCode || exitCode === 0) console.log(exitCode);
  try {
    await node.destroy([options]);
  } catch (e) {
    console.error(e);
  }
  if (options.exit) process.exit(1);
}

process.on('exit', exitHandler.bind(null, { cleanup: true }));
process.on('SIGINT', exitHandler.bind(null, { exit: true }));
process.on('SIGUSR1', exitHandler.bind(null, { exit: true }));
process.on('SIGUSR2', exitHandler.bind(null, { exit: true }));
process.on('uncaughtException', exitHandler.bind(null, { exit: true }));
