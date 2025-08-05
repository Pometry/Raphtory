import 'dotenv/config';
import { InstancesClient } from '@google-cloud/compute';
import { NodeSSH } from 'node-ssh';
import assert from 'node:assert';
import fs from 'node:fs';
import { homedir, userInfo } from 'node:os';
import { execSync } from 'node:child_process';

const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
assert(GITHUB_TOKEN !== undefined);

const time = new Date().getTime();
const name = `graphql-benchmark-${time}`;
const zone = 'europe-west2-a';
const project = 'tether-435314';
const machineType = 'e2-standard-32';
const imageProject = 'debian-cloud';
const imageFamily = 'debian-12';

export async function setupServer() {
    const commit = execSync('git rev-parse HEAD', { encoding: 'utf8' });
    const username = userInfo().username;
    const home = homedir(); // use ~ instead
    const publicKeyPath = `${home}/.ssh/id_rsa.pub`;
    const publicKey = fs.readFileSync(publicKeyPath, 'utf8').trim();

    const client = new InstancesClient();
    const diskSizeGb = 50;
    await client.insert({
        project,
        zone,
        instanceResource: {
            name,
            machineType: `zones/${zone}/machineTypes/${machineType}`,
            disks: [
                {
                    boot: true,
                    autoDelete: true,
                    initializeParams: {
                        sourceImage: `projects/${imageProject}/global/images/family/${imageFamily}`,
                        diskSizeGb: diskSizeGb.toString(),
                    },
                },
            ],
            networkInterfaces: [
                {
                    network: 'global/networks/default',
                    accessConfigs: [
                        {
                            type: 'ONE_TO_ONE_NAT',
                            name: 'External NAT',
                        },
                    ],
                },
            ],
            metadata: {
                items: [
                    {
                        key: 'enable-oslogin',
                        value: 'FALSE', // before was set to TRUE
                    },
                    {
                        key: 'ssh-keys',
                        value: `${username}:${publicKey}`,
                    },
                ],
            },
        },
    });

    const address = await executeUntilSucess(async () => {
        const [instance] = await client.get({
            project,
            zone,
            instance: name,
        });
        const externalIp = instance.networkInterfaces
            ?.at(0)
            ?.accessConfigs?.at(0)?.natIP;
        if (typeof externalIp !== 'string') {
            throw new Error("Couldn't get instance address");
        }
        return externalIp;
    });

    // const exec = (cmd: string) => {};

    const ssh = new NodeSSH();
    await executeUntilSucess(async () => {
        await ssh.connect({
            host: address,
            username: username,
            privateKeyPath: `${home}/.ssh/id_rsa`,
        });
    });

    //     const pyenvLines = `
    // export PYENV_ROOT="$HOME/.pyenv"
    // [[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
    // eval "$(pyenv init - bash)"
    // eval "$(pyenv virtualenv-init -)"
    // `;

    const setupCommands = [
        'sudo apt-get update',
        'sudo apt-get install -y git docker.io',
        // `sudo usermod -aG docker ${username} && newgrp docker && su - ${username}`,
        // `curl https://sh.rustup.rs -sSf | sh -s -- -y`,
        // 'curl -fsSL https://pyenv.run | bash',
        // `echo '${pyenvLines}' >> ~/.bashrc`,
        // 'pyenv install 3.12.0',
        // 'pyenv global 3.12.0',
        `git clone https://${GITHUB_TOKEN}@github.com/pometry/pometry-ui.git`,
    ];
    for (const cmd of setupCommands) {
        console.log(`$ ${cmd}`);
        const result = await ssh.execCommand(cmd);
        console.log(result);
    }

    // TODO: wrote all of these commands into a script and simply run it
    const repoCommadns = [
        `git checkout ${commit}`,
        'git config -f .gitmodules submodule.applications/apache/custom_raphtory/raphtory.url https://github.com/Pometry/raphtory.git',
        'git submodule sync',
        'GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no" git submodule update --init',
        'sudo docker build -t server -f bench/docker/apache.Dockerfile .',
        'sudo docker run -p 1736:1736 server',
        // 'pip install maturin',
        // 'make install && make graphs && make run', // FIXME: Im getting cargo not found
        // 'python -m venv venv',
        // 'source venv/bin/activate && pip install maturin',
        // 'source venv/bin/activate && make install && make graphs && make run', // FIXME: Im getting cargo not found
    ];
    for (const cmd of repoCommadns) {
        console.log(`$ ${cmd}`);
        const result = await ssh.execCommand(`cd pometry-ui && ${cmd}`);
        console.log(result);
    }
}

async function executeUntilSucess<T>(exec: () => Promise<T>) {
    let attempts = 0;
    // 600 seconds
    while (attempts < 600) {
        try {
            return await exec();
        } catch {
            attempts++;
            await new Promise((resolve) => setTimeout(resolve, 1000)); // 1 second
        }
    }
}

setupServer();
